'use strict';

const async = require('async');
const _ = require('lodash');
const hbase = require('hbase');
//const krb5 = require ('krb5');
const fetch = require('node-fetch');
const versions = require('../package.json').contributes.target.versions;
const colFamConfig = require('./columnFamilyConfig');

var client = null;
var state = {
	connectionInfo: {}
};
var clientKrb = null;



module.exports = {
	connect: function(connectionInfo, logger, cb){
		if(!client){
			logger.log('info', connectionInfo);
			let options = {
				host: connectionInfo.host,
				port: connectionInfo.port
			};

			if(connectionInfo.principal){
				options = setAuthData(options, connectionInfo);

				// krb5(options.krb5, (err, krb) => {
				// 	if (err) {
				// 		return cb(err);
				// 	}
				// 	clientKrb = krb;
				// 	client = hbase(options);
				// 	return cb();
				// });
			}

			client = hbase(options);
			return cb();
		}
		return cb();
	},

	disconnect: function(cb){
		client = null;
		state.connectionInfo = {};
		cb();
	},

	testConnection: function(connectionInfo, logger, cb){
		this.connect(connectionInfo, logger, err => {
			if(err){
				logger.log('error', err);
				return cb(err);
			}

			getClusterVersion(connectionInfo).then(version => {
				return cb();
			})
			.catch(err => {
				logger.log('error', err);
				return cb(err);
			});
		});
	},

	getDbCollectionsNames: function(connectionInfo, logger, cb) {
		this.connect(connectionInfo, logger, err => {
			if(err){
				return cb(err);
			}
			state.connectionInfo = connectionInfo;

			getNamespacesList(connectionInfo).then(namespaces => {
				async.map(namespaces, (namespace, callback) => {
					getTablesList(connectionInfo, namespace)
						.then(res => {
							return callback(null, res);
						})
						.catch(err => {
							return callback(err);
						});
				}, (err, items) => {
					items = prepareDataItems(namespaces, items);
					return cb(err, items);
				});
			})
			.catch(err => {
				logger.log('error', err);
				return cb(err);
			});
		});
	},

	getDbCollectionsData: function(data, logger, cb){
		let { recordSamplingSettings, fieldInference } = data;
		let namespaces = data.collectionData.dataBaseNames;
		let info = { 
			host: state.connectionInfo.host,
			port: state.connectionInfo.port
		};

		async.map(namespaces, (namespace, callback) => {
			let tables = data.collectionData.collections[namespace];

			async.map(tables, (table, tableCallback) => {
				let currentSchema;

				getClusterVersion(state.connectionInfo)
					.then(version => {
						info.version = handleVersion(version, versions) || '';
						return getTableSchema(namespace, table, state.connectionInfo)
					})
					.then(schema => {
						currentSchema = schema;
						return scanDocuments(namespace, table, recordSamplingSettings);
					})
					.then(rows => {
						let documentsPackage = {
							dbName: namespace,
							collectionName: table,
							emptyBucket: true
						};

						if(rows.length){
							//let size = getSampleDocSize(rows.length, recordSamplingSettings);
							let handledRows = handleRows(rows);
							let customSchema = setColumnProps(handledRows.schema, currentSchema);

							documentsPackage.emptyBucket = false;
							documentsPackage.documents = handledRows.documents;
							documentsPackage.validation = {
								jsonSchema: customSchema
							}
						}

						return tableCallback(null, documentsPackage);
					})
					.catch(err => {
						logger.log('error', err);
						return tableCallback(err);
					});
			}, (err, items) => {
				if(err){
					logger.log('error', err);
				}
				return callback(err, items);
			});
		}, (err, res) => {
			if(err){
				logger.log('error', err);
			}
			return cb(err, res, info);
		});
	}
};

function getHostURI(connectionInfo){
	let query = `http://${connectionInfo.host}:${connectionInfo.port}`;
	return query;
}


function getRequestOptions(connectionInfo){
	let headers = {
		'Cache-Control': 'no-cache',
		'Accept': 'application/json'
	};

	if(connectionInfo.principal){
		let credentials = `${connectionInfo.userName}:${connectionInfo.password}`;
		let encodedCredentials = new Buffer(credentials).toString('base64');

		clientKrb.token((err, token) => {
			if (err) {
				return { err };
			}
		})
		headers.Authorization = `Negotiate ${token}`;
	}

	return {
		'method': 'GET',
		'headers': headers
	};
}

function fetchRequest(query, connectionInfo){
	let options = getRequestOptions(connectionInfo);
	let response;

	if(options.error){
		return new Promise((reject, resolve) => {
			reject(options.error);
		});
	}

	return fetch(query, options)
		.then(res => {
			response = res;
			return res.text();
		})
		.then(body => {
			body = JSON.parse(body);

			if(!response.ok){
				throw {
					message: response.statusText, code: response.status, description: body
				};
			}
			return body;
		});
}

function getNamespacesList(connectionInfo){
	let query = `${getHostURI(connectionInfo)}/namespaces`;

	return fetchRequest(query, connectionInfo).then(res => {
		return res.Namespace.filter(item => item !== 'hbase');
	});
}

function getTablesList(connectionInfo, namespace){
	let query = `${getHostURI(connectionInfo)}/namespaces/${namespace}/tables`;

	return fetchRequest(query, connectionInfo).then(res => {
		return res;
	});
}

function prepareDataItems(namespaces, items){
	return items.map((item, index) => {
		return {
			dbName: namespaces[index],
			dbCollections: item.table.map(table => {
				return table.name;
			})
		};
	}); 
}

function getTableSchema(namespace, table, connectionInfo){
	let query = `${getHostURI(connectionInfo)}/${namespace}:${table}/schema`;

	return fetchRequest(query, connectionInfo).then(res => {
		return res;
	});
}

function getClusterVersion(connectionInfo){
	let query = `${getHostURI(connectionInfo)}/version/cluster`;

	return fetchRequest(query, connectionInfo).then(res => {
		return res;
	});
}

function scanDocuments(namespace, table, recordSamplingSettings){
	let options = {};
	
	if(recordSamplingSettings.active === 'absolute'){
		let size = recordSamplingSettings.absolute;
		options.filter = {
			type: 'PageFilter',
			value: size 
		};
	}

	return new Promise((resolve, reject) => {
		client
		.table(`${namespace}:${table}`)
		.scan(options, (err, rows) => {
			if(err){
				reject(err);
			}
			resolve(rows);
		});
	});
}

function handleRows(rows, size){
	let data = {
		hashTable: {},
		documents: [],
		schema: {
			properties: {
				'^[a-zA-Z0-9_.-]*$': {
					type: 'string',
					primaryKey: true,
					isPatternField: true
				}
			}
		}
	};

	rows.slice(-size).forEach(item => {
		if(!data.hashTable[item.key]){
			let handledColumn = handleColumn(item, data.schema.properties);
			data.schema.properties = handledColumn.schema;
			data.documents.push(handledColumn.doc);
			data.hashTable[item.key] = data.documents.length - 1;
		}

		let index = data.hashTable[item.key];
		let handledColumn = handleColumn(item, data.schema.properties, data.documents[index]);
		data.documents[index] = handledColumn.doc;
		data.schema.properties = handledColumn.schema;
	});

	return data;	
}

function handleColumn(item, schema, doc = {}){
	let columnData = item.column.split(':');
	let columnFamily = columnData[0];
	let columnQualifier = columnData[1];

	if(!doc[columnFamily]){
		doc[columnFamily] = {};
	}

	if(!schema[columnFamily]){
		schema[columnFamily] = {
			type: 'colFam',
			properties: {}
		};
	}

	if(!schema[columnFamily].properties[columnQualifier]){
		schema[columnFamily].properties[columnQualifier] = {
			type: 'colQual',
			properties: {
				value: {
					type: getValueType(item.$)
				}
			}
		};
	}

	doc[columnFamily][columnQualifier] = {
		value: getValue(item.$, schema[columnFamily].properties[columnQualifier]),
		'timestamp': item.timestamp
	};

	return { doc, schema };
}

function getValueType(value){
	try {
		value = JSON.parse(value);
		return _.isArray(value) ? 'array' : 'object' 
	} catch (err) {
		return 'byte';
	}
}

function getValue(value, colQual){
	let schemaValue = colQual.properties.value;

	try {
		value = JSON.parse(value);
		return value;
	} catch (err) {
		schemaValue.type = 'byte';
		return value;
	}
}


function parseSchema(schema){
	schema = schema.replace('=>', ':');

	try {
		schema = JSON.parse(schema);
	} catch (err) {
		schema = null;
	}

	return schema;
}

function setColumnProps(customSchema, schema){
	schema.ColumnSchema.forEach(item => {
		if (!customSchema.properties[item.name]){
			customSchema.properties[item.name] = {
				type: 'colFam'
			};
		}

		if(colFamConfig && colFamConfig.length){
			colFamConfig.forEach(prop => {
				switch(prop.propertyType){
					case 'number':
						customSchema.properties[item.name][prop.propertyKeyword] = Number(item[prop.schemaKeyword]);
						break;
					case 'boolean':
						customSchema.properties[item.name][prop.propertyKeyword] = getBoolean(item[prop.schemaKeyword]);
						break;
					default:
						customSchema.properties[item.name][prop.propertyKeyword] = item[prop.schemaKeyword];
				}
			});
		}
	});

	return customSchema;
}

function getBoolean(value){
	return value === 'TRUE';
}

function getSampleDocSize(count, recordSamplingSettings) {
	let per = recordSamplingSettings.relative.value;
	let res = (recordSamplingSettings.active === 'absolute')
		? recordSamplingSettings.absolute.value
		: Math.ceil( count/100 * per);
	return count < res ? count : res;
}

function handleVersion(version, versions){
	return versions.find(item => {
		let sItem = item.split('');

		sItem = sItem.map((item, index) => {
			return item === 'x' ? version[index] : item;
		});
		return version	=== sItem.join('')	
	})
}

function setAuthData(options, connectionInfo){
	let authParams = {
		krb5:{
			principal: connectionInfo.principal,
			service_principal: connectionInfo.service_principal,
			[connectionInfo.auth]: connectionInfo[connectionInfo.auth]
		}
	};

	options = Object.assign(options, authParams);

	return options;
}
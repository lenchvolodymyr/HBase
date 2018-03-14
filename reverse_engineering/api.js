'use strict';

const async = require('async');
const _ = require('lodash');
const hbase = require('hbase');
const fetch = require('node-fetch');

var client = null;
var state = {
	connectionInfo: null
};

module.exports = {
	connect: function(connectionInfo, logger, cb){
		if(!client){
			let options = {
				host: connectionInfo.host,
				port: connectionInfo.post
			};

			client = hbase({ options });
			return cb();
		}
		return cb();
	},

	disconnect: function(connectionInfo, cb){
		cb();
	},

	testConnection: function(connectionInfo, logger, cb){
		this.connect(connectionInfo, logger, err => {
			if(err){
				return cb(err);
			}

			client.version((error, version) => {
				if(error){
					return cb(error);
				}
				return cb(null)
			});
		});
	},

	getDbCollectionsNames: function(connectionInfo, logger, cb) {
		this.connect(connectionInfo, logger, err => {
			if(err){
				return cb(err);
			}

			state.connectionInfo = connectionInfo;


			getNamespacesList(connectionInfo, (err, res) => {
				if(err){
					logger.log('error', err);
					cb(err);
				} else {
					let namespaces = res.Namespace;

					async.map(namespaces, (namespace, callback) => {
						getTablesList(connectionInfo, namespace, callback);
					}, (err, items) => {
						items = prepareDataItems(namespaces, items);
						cb(err, items);
					});
				}
			});
		});
	},

	getDbCollectionsData: function(data, logger, cb){
		let { recordSamplingSettings, fieldInference } = data;
		let size = getSampleDocSize(1000, recordSamplingSettings) || 1000;
		let namespaces = data.collectionData.dataBaseNames;

		async.map(namespaces, (namespace, callback) => {
			let tables = data.collectionData.collections[namespace];

			async.map(tables, (table, tableCallback) => {
				getTableSchema(namespace, table, state.connectionInfo, logger, (err, schema) => {
					if(err){
						logger.log('error', err);
						return tableCallback(err);
					}

					scanDocuments(table, (err, rows) => {
						if(err){
							logger.log('error', err);
							return tableCallback(err);
						}


						let handledRows = handleRows(rows);
						let customSchema = handledRows.schema;
						customSchema = setTTL(customSchema, schema);

						let documentsPackage = {
							dbName: namespace,
							collectionName: table,
							emptyBucket: !handledRows.documents.length,
							documents: handledRows.documents,
							validation: {
								jsonSchema: customSchema
							}
						};

						return tableCallback(null, documentsPackage);
					});
				});
			}, (err, items) => {
				if(err){
					logger.log('error', err);
				}
				return callback(err, items);
			});
		}, cb);
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

	if(connectionInfo.useAuth && connectionInfo.userName && connectionInfo.password){
		let credentials = `${connectionInfo.userName}:${connectionInfo.password}`;
		let encodedCredentials = new Buffer(credentials).toString('base64');
		headers.Authorization = `Basic ${encodedCredentials}`;
	}

	return {
		'method': 'GET',
		'headers': headers
	};
}

function fetchRequest(query, connectionInfo, logger){
	let options = getRequestOptions(connectionInfo);
	let response;

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

function getNamespacesList(connectionInfo, cb){
	let query = `${getHostURI(connectionInfo)}/namespaces`;

	return fetchRequest(query, connectionInfo).then(res => {
		return cb(null, res);
	})
	.catch(err => {
		return cb(err);
	});
}

function getTablesList(connectionInfo, namespace, cb){
	let query = `${getHostURI(connectionInfo)}/namespaces/${namespace}/tables`;

	return fetchRequest(query, connectionInfo).then(res => {
		return cb(null, res);
	})
	.catch(err => {
		return cb(err);
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

function getTableSchema(namespace, table, connectionInfo, logger, cb){
	let query = `${getHostURI(connectionInfo)}/${table}/schema`;

	return fetchRequest(query, connectionInfo, logger).then(res => {
		return cb(null, res);
	})
	.catch(err => {
		return cb(err);
	});
}

function scanDocuments(table, cb){
	client
	.table(table)
	.scan(cb);
}

function handleRows(rows){
	let data = {
		hashTable: {},
		documents: [],
		schema: {
			properties: {}
		}
	};

	rows.forEach(item => {
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

	doc[columnFamily][columnQualifier] = {
		value: item.$,
		'^Timestamp[0-9]+$': item.timestamp
	};

	schema[columnFamily].properties[columnQualifier] = {
		type: 'colQual',
		properties: {
			'^Timestamp[0-9]+$': {
				type: 'byte',
				isPatternField: true
			}
		}
	};

	return { doc, schema };
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

function setTTL(customSchema, schema){
	schema.ColumnSchema.forEach(item => {
		if (!customSchema.properties[item.name]){
			customSchema.properties[item.name] = {
				type: 'colFam'
			};
		}

		customSchema.properties[item.name].ttl = item.TTL;
	});

	return customSchema;
}

function getSampleDocSize(count, recordSamplingSettings) {
	let per = recordSamplingSettings.relative.value;
	return (recordSamplingSettings.active === 'absolute')
		? recordSamplingSettings.absolute.value
			: Math.round( count/100 * per);
}
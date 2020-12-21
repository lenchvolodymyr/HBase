'use strict';

const async = require('async');
const _ = require('lodash');
const fetch = require('node-fetch');
const versions = require('../package.json').contributes.target.versions;
const colFamConfig = require('./columnFamilyConfig');
const kerberosService = require('./kerberosService');
var state = {
	connectionInfo: {}
};
var clientKrb = null;
const DEFAULT_NAMESPACE = 'No Namespace'

module.exports = {
	connect: function(connectionInfo, logger, cb, app){
		const kerberos = app.require('kerberos');
		logger.log('info', connectionInfo, 'Connection information', connectionInfo.hiddenKeys);

		let options = setAuthData({
			host: connectionInfo.host,
			port: connectionInfo.port
		}, connectionInfo);
		
		if(!clientKrb && options.krb5){
			logger.log('info', Object.assign({}, options.krb5, { platform: process.platform }), 'Kerberos options', connectionInfo.hiddenKeys);

			kerberosService({ kerberos }).getClient(options.krb5)
				.then(client => {
					clientKrb = client;
					return cb();
				}, err => cb(err));
		} else {
			return cb();
		}
	},

	disconnect: function(cb){
		client = null;
		state.connectionInfo = {};
		if (clientKrb) {
			clientKrb.destroy(cb);
		} else {
			cb();
		}
	},

	testConnection: function(connectionInfo, logger, cb, app){
		logger.clear();
		
		this.connect(connectionInfo, logger, err => {
			if(err){
				logger.log('error', err, 'Test connection', connectionInfo.hiddenKeys);
				return cb(err);
			}

			getClusterVersion(connectionInfo, logger).then(version => {
				return cb();
			})
			.catch(err => {
				return logError(logger, cb)(err, 'Test connection', connectionInfo.hiddenKeys);
			});
		}, app);
	},

	getDbCollectionsNames: function(connectionInfo, logger, cb, app) {
		logger.clear();

		this.connect(connectionInfo, logger, err => {
			if(err){
				return logError(logger, cb)(err, 'Connection error', connectionInfo.hiddenKeys);
			}
			state.connectionInfo = connectionInfo;

			getNamespacesList(connectionInfo, logger).then(namespaces => {
				async.mapSeries(namespaces, (namespace, callback) => {
					getTablesList(connectionInfo, namespace, logger)
						.then(res => {
							return callback(null, res);
						}, (err) => {
							return callback(err);
						});
				}, (err, items) => {
					if (err) {
						return logError(logger, cb)(err, 'Get tables names error', connectionInfo.hiddenKeys);
					}

					items = prepareDataItems(namespaces, items);

					return cb(err, items);
				});
			})
			.catch(err => {
				logError(logger, cb)(err, 'Get tables names error', connectionInfo.hiddenKeys);
			});
		}, app);
	},

	getDbCollectionsData: function(data, logger, cb){
		let { recordSamplingSettings, fieldInference, includeEmptyCollection } = data;
		let namespaces = data.collectionData.dataBaseNames;
		let info = { 
			host: state.connectionInfo.host,
			port: Number(state.connectionInfo.port)
		};

		async.map(namespaces, (namespace, callback) => {
			let tables = data.collectionData.collections[namespace];

			if(!tables){
				let documentsPackage = {
					dbName: namespace,
					emptyBucket: true
				};
				return callback(null, documentsPackage);
			} else {
				async.mapSeries(tables, (table, tableCallback) => {
					let currentSchema;
					let documentsPackage = {
						dbName: namespace
					};
					logger.progress({ message: 'Start getting version of cluster', containerName: namespace, entityName: table });
					getClusterVersion(state.connectionInfo)
						.then(version => {
							logger.progress({ message: 'Version of cluster: ' + version, containerName: namespace, entityName: table });

							info.version = handleVersion(version, versions) || '';

							logger.progress({ message: 'Start getting schema of table', containerName: namespace, entityName: table });

							return getTableSchema(namespace, table, state.connectionInfo)
						})
						.then(schema => {
							logger.progress({ message: 'Schema has successfully got!', containerName: namespace, entityName: table });
							logger.progress({ message: 'Start getting documents', containerName: namespace, entityName: table });

							currentSchema = schema;
							return scanDocuments(namespace, table, recordSamplingSettings, state.connectionInfo);
						})
						.then(rows => {
							logger.progress({ message: 'Documents have successfully got!', containerName: namespace, entityName: table });

							documentsPackage.collectionName = table;

							if(rows.length){
								let handledRows = handleRows(rows);
								let customSchema = setColumnProps(handledRows.schema, currentSchema);

								if(fieldInference.active === 'field'){
									documentsPackage.documentTemplate = handledRows.documents[0];
								}

								documentsPackage.documents = handledRows.documents;
								documentsPackage.validation = {
									jsonSchema: customSchema
								}
							} else if (currentSchema) {
								let customSchema = setColumnProps({ properties: {} }, currentSchema);
								documentsPackage.documents = [];
								documentsPackage.validation = {
									jsonSchema: customSchema
								};
							} else if(includeEmptyCollection){
								documentsPackage.documents = [];
							} else {
								documentsPackage = null;
							}

							return tableCallback(null, documentsPackage);
						})
						.catch(err => {
							return tableCallback(err, []);
						});
				}, (err, items) => {
					if(!err){
						items = items.filter(item => item);
					}
					return callback(err, items);
				});
			}
		}, (err, res) => {
			if (err) {
				return logError(logger, cb)(err, 'Get data error', state.connectionInfo.hiddenKeys);
			} else {
				return cb(err, res, info);
			}
		});
	}
};

function getHostURI(connectionInfo){
	const protocol = connectionInfo.https ? 'https' : 'http';
	let query = `${protocol}://${connectionInfo.host}:${connectionInfo.port}`;
	return query;
}


function getRequestOptions(data, logger) {
	return new Promise((resolve, reject) => {
		let headers = {
			'Cache-Control': 'no-cache',
			'Accept': 'application/json'
		};
	
		if (clientKrb) {
			clientKrb.token((err, token) => {
				if (err) {
					return reject(err);
				}

				if (logger) {
					logger.log('info', { token });
				}

				headers.Authorization = `Negotiate ${token}`;
	
				resolve({
					method: 'GET',
					headers
				});
			});
		} else {
			resolve({
				'method': 'GET',
				'headers': headers
			});
		}
	});
}

function fetchRequest(query, connectionInfo, logger){
	return getRequestOptions(connectionInfo, logger)
		.then((options) => {
			return fetch(query, options)
		})
		.then(handleResponse)
		.then(({ result, response}) => {
			return JSON.parse(result);
		});
}

const getTableNames = (connectionInfo, logger) => {
	let query = `${getHostURI(connectionInfo)}/`;

	return fetchRequest(query, connectionInfo, logger).then(res => {
		return _.get(res, 'table', []).map(table => table.name);
	});
};

const splitTableName = name => name.indexOf(':') !== -1 ? name.split(':') : ['', name];

const getNamespacesFromTables = (tables) => {
	return Promise.resolve(_.uniq(tables.map((tableName) => splitTableName(tableName || '').shift())));
};

function getNamespacesList(connectionInfo, logger){
	let query = `${getHostURI(connectionInfo)}/namespaces`;

	return fetchRequest(query, connectionInfo, logger).then(res => {
		return res.Namespace;
	}, err => {
		const areNamespacesNotAllowed = (err.code === 405);

		if (areNamespacesNotAllowed) {
			return getTableNames(connectionInfo, logger).then(getNamespacesFromTables);
		} else {
			return Promise.reject(err);
		}
	}).then(res => {
		return res.filter(item => item !== 'hbase');
	});
}

function getTablesList(connectionInfo, namespace, logger){
	let query = `${getHostURI(connectionInfo)}/namespaces/${namespace}/tables`;

	return fetchRequest(query, connectionInfo, logger).then(res => {
		return res;
	}, err => {
		const areNamespacesNotAllowed = (err.code === 404);

		if (areNamespacesNotAllowed) {
			return getTableNames(connectionInfo, logger).then(filterTables.bind(null, namespace)).then(tableNames => ({ table: tableNames }));
		} else {
			return Promise.reject(err);
		}
	});
}

const filterTables = (namespace, tableNames) => {
	return tableNames.map(splitTableName)
		.filter(([ namespaceName ]) => namespaceName === namespace)
		.map(([ns, name]) => ({ name }));
};

function prepareDataItems(namespaces, items){
	return items.map((item, index) => {
		return {
			dbName: namespaces[index] || DEFAULT_NAMESPACE,
			dbCollections: item.table.map(table => {
				return table.name;
			})
		};
	}); 
}

function getTableSchema(namespace, table, connectionInfo){
	let query = `${getHostURI(connectionInfo)}/${getNamespaceTableName(namespace, table)}/schema`;

	return fetchRequest(query, connectionInfo).then(res => {
		return res;
	});
}

function getClusterVersion(connectionInfo, logger){
	let query = `${getHostURI(connectionInfo)}/version/cluster`;

	return fetchRequest(query, connectionInfo, logger).then(res => {
		return res;
	});
}

const getNamespaceTableName = (namespace, table) => (namespace && namespace !== DEFAULT_NAMESPACE) ? `${namespace}:${table}` : table;

function handleRows(rows){
	let data = {
		hashTable: {},
		documents: [],
		schema: {
			properties: {
				'Row Key': {
					type: 'string',
					key: true,
					pattern: '^[a-zA-Z0-9_.-]*$'
				}
			}
		}
	};

	rows.forEach(item => {
		if(!data.hashTable.hasOwnProperty(item.key)){
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

	doc['Row Key'] = '';

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
		schema[columnFamily].properties[columnQualifier] = getColumnQualSchema(item);
	}

	doc[columnFamily][columnQualifier] = [{
		'timestamp': item.timestamp + '',
		value: getValue(item.$, schema[columnFamily].properties[columnQualifier])
	}];

	return { doc, schema };
}

function getColumnQualSchema(item){
	return {
		type: 'colQual',
		items: {
			type: 'object',
			properties:{
				timestamp: {
					type: 'string',
					pattern: '^[0-9]+$'
				},
				value: {
					type: getValueType(item.$)
				}
			}
		}
	};
}

function getValueType(value){
	try {
		value = JSON.parse(value);

		switch(typeof value) {
			case 'object':
				if (value) {
					return _.isArray(value) ? 'array' : 'object' 
				} else {
					return 'null';
				}
			case 'number':
				return 'number';
			case 'string':
				return 'string';
			case 'boolean':
				return 'boolean';
			default:
				return 'byte';
		}

	} catch (err) {
		return 'byte';
	}
}

function getValue(value, colQual){
	let schemaValue = colQual.items.properties.value;

	try {
		value = JSON.parse(value);
		return value;
	} catch (err) {
		schemaValue.type = 'byte';
		return value;
	}
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
	let authParams = {};

	if (connectionInfo.auth === 'kerberos') {
		authParams.krb5 = {
			principal: connectionInfo.principal,
			service_principal: 'HTTP' + (process.platform === 'win32' ? '/' : '@') + connectionInfo.host,
			password: connectionInfo.password
		};
	}

	options = Object.assign(options, authParams);

	return options;
}

const handleResponse = (response) => {
	return response.text()
		.then(result => {
			if (!response.ok) {
				return Promise.reject({
					message: response.statusText, code: response.status, description: result
				});
			}

			return {
				response,
				result
			};			
		})
};

const getScannerBody = (recordSamplingSettings) => {
	const t = (size = 4) => ' '.repeat(size);
	const getFilter = (filter) => {
		return '\n' + t() + '<filter>\n' + t(2*4) + JSON.stringify(filter, null, 4).split('\n').join('\n' + t(2*4)) + '\n' + t() + '</filter>\n';
	};
	let body = '<Scanner batch="1000">';

	if (recordSamplingSettings.active === 'absolute') {
		let size = recordSamplingSettings.absolute.value;

		body += getFilter({
			type: 'PageFilter',
			value: size 
		});
	}

	body += '</Scanner>';

	return body;
};

const scanDocuments = (namespace, table, recordSamplingSettings, connectionInfo) => {
	const tableName = getNamespaceTableName(namespace, table);
	let query = `${getHostURI(connectionInfo)}/${tableName}/scanner`;

	return getRequestOptions()
		.then(options => {
			options.method = 'PUT';
			options.body = getScannerBody(recordSamplingSettings);
			options.headers.Accept = 'text/xml';
			options.headers['Content-Type'] = 'text/xml';

			return fetch(query, options);
		})
		.then(handleResponse)
		.then(({ response, result }) => {
			return response.headers.get('location') || '';
		})
		.then(getCells);
};

const getCells = (query, cells = []) => new Promise((resolve, reject) => {
	return getRequestOptions()
		.then(options => {
			return fetch(query, options);
		})
		.then(handleResponse)
		.then(({ result, response }) => {
			if (response.status === 204) {
				resolve(cells);
			} else {
				const data = getRows(JSON.parse(result))

				return getCells(query, [ ...cells, ...data ]);
			}
		})
		.then(resolve, reject);
});

const decodeBase64 = (str) => Buffer.from(str, 'base64').toString();

const getRows = (data) => {
	let cells = [];

	data.Row.forEach((row) => {
		let key = decodeBase64(row.key, 'utf-8');

		return row.Cell.forEach((cell) => {
		  data = {};
		  data.key = key;
		  data.column = decodeBase64(cell.column, 'utf-8');
		  data.timestamp = cell.timestamp;
		  data.$ = decodeBase64(cell.$, 'utf-8');

		  return cells.push(data);
		});
	});

	return cells;
};

const logError = (logger, cb) => (err, subject, hiddenKeys) => {
	logger.log('error', err, subject, hiddenKeys);

	setTimeout(() => {
		cb(err);
	}, 1000);
};

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

						let documentsPackage = {
							dbName: namespace,
							collectionName: table,
							emptyBucket: !handledRows.documents.length,
							documents: handledRows.documents,
							validation: {
								jsonSchema: handledRows.schema
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
			let handledColumn = handleColumn(item);
			data.schema.properties = handledColumn.schema;
			data.documents.push(handledColumn.doc);
			data.hashTable[item.key] = data.documents.length - 1;
		}

		let index = data.hashTable[item.key];
		let handledColumn = handleColumn(item, data.documents[index], data.schema.properties);
		data.documents[index] = handledColumn.doc;
		data.schema.properties = handledColumn.schema;
	});

	return data;	
}

function handleColumn(item, doc = {}, schema = {}){
	let columnData = item.column.split(':');
	let columnFamily = columnData[0];
	let columnQualifier = columnData[1];

	if(!doc[columnFamily]){
		doc[columnFamily] = {};
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




function readCollectionById(dbLink, collectionId, callback) {
	var collLink = `dbs/${dbLink}/colls/${collectionId}`;

	client.readCollection(collLink, function (err, coll) {
		if (err) {
			console.log(err);
			callback(err);
		} else {
			callback(null, coll);
		}
	});
}

function getOfferType(collection, callback) {
	var querySpec = {
		query: 'SELECT * FROM root r WHERE  r.resource = @link',
		parameters: [
			{
				name: '@link',
				value: collection._self
			}
		]
	};

	client.queryOffers(querySpec).toArray(function (err, offers) {
		if (err) {
			callback(err);

		} else if (offers.length === 0) {
			callback('No offer found for collection');

		} else {
			var offer = offers[0];
			callback(null, offer);
		}
	});
}

function listDatabases(callback) {
	var queryIterator = client.readDatabases().toArray(function (err, dbs) {
		if (err) {
			callback(err);
		}

		callback(null, dbs);
	});
}

function listCollections(databaseLink, callback) {
	var queryIterator = client.readCollections(databaseLink).toArray(function (err, cols) {
		if (err) {
			callback(err);
		} else {            
			callback(null, cols);
		}
	});
}

function readDatabaseById(databaseId, callback) {
	client.readDatabase('dbs/' + databaseId, function (err, db) {
		if (err) {
			callback(err);
		} else {
			callback(null, db);
		}
	});
}

function listDocuments(collLink, maxItemCount, callback) {
	var queryIterator = client.readDocuments(collLink, { maxItemCount }).toArray(function (err, docs) {
		if (err) {
			callback(err);
		} else {
			callback(null, docs);
		}
	});
}

function filterDocuments(documents){
	return documents.map(item =>{
		for(let prop in item){
			if(prop && prop[0] === '_'){
				delete item[prop];
			}
		}
		return item;
	});
}

function generateCustomInferSchema(bucketName, documents, params){
	function typeOf(obj) {
		return {}.toString.call(obj).split(' ')[1].slice(0, -1).toLowerCase();
	};

	let sampleSize = params.sampleSize || 30;

	let inferSchema = {
		"#docs": 0,
		"$schema": "http://json-schema.org/schema#",
		"properties": {}
	};

	documents.forEach(item => {
		inferSchema["#docs"]++;
		
		for(let prop in item){
			if(inferSchema.properties.hasOwnProperty(prop)){
				inferSchema.properties[prop]["#docs"]++;
				inferSchema.properties[prop]["samples"].indexOf(item[prop]) === -1 && inferSchema.properties[prop]["samples"].length < sampleSize? inferSchema.properties[prop]["samples"].push(item[prop]) : '';
				inferSchema.properties[prop]["type"] = typeOf(item[prop]);
			} else {
				inferSchema.properties[prop] = {
					"#docs": 1,
					"%docs": 100,
					"samples": [item[prop]],
					"type": typeOf(item[prop])
				}
			}
		}
	});

	for (let prop in inferSchema.properties){
		inferSchema.properties[prop]["%docs"] = Math.round((inferSchema.properties[prop]["#docs"] / inferSchema["#docs"] * 100), 2);
	}
	return inferSchema;
}

function getDocumentKindDataFromInfer(data, probability){
	let suggestedDocKinds = [];
	let otherDocKinds = [];
	let documentKind = {
		key: '',
		probability: 0	
	};

	if(data.isCustomInfer){
		let minCount = Infinity;
		let inference = data.inference.properties;

		for(let key in inference){
			if(config.excludeDocKind.indexOf(key) === -1){
				if(inference[key]["%docs"] >= probability && inference[key].samples.length && typeof inference[key].samples[0] !== 'object'){
					suggestedDocKinds.push(key);

					if(inference[key]["%docs"] >= documentKind.probability && inference[key].samples.length < minCount){
						minCount = inference[key].samples.length;
						documentKind.probability = inference[key]["%docs"];
						documentKind.key = key;
					}
				} else {
					otherDocKinds.push(key);
				}
			}
		}
	} else {
		let flavor = (data.flavorValue) ? data.flavorValue.split(',') : data.inference[0].Flavor.split(',');
		if(flavor.length === 1){
			suggestedDocKinds = Object.keys(data.inference[0].properties);
			let matсhedDocKind = flavor[0].match(/([\s\S]*?) \= "?([\s\S]*?)"?$/);
			documentKind.key = (matсhedDocKind.length) ? matсhedDocKind[1] : '';
		}
	}

	let documentKindData = {
		bucketName: data.bucketName,
		documentList: suggestedDocKinds,
		documentKind: documentKind.key,
		preSelectedDocumentKind: data.preSelectedDocumentKind,
		otherDocKinds
	};

	return documentKindData;
}

function handleBucket(connectionInfo, collectionNames, database, dbItemCallback){
	let size = getSampleDocSize(1000, connectionInfo.recordSamplingSettings) || 1000;

	async.map(collectionNames, (collectionName, collItemCallback) => {
		readCollectionById(database.id, collectionName, (err, collection) => {
			if(err){
				console.log(err);
			} else {
				listDocuments(collection._self, size, (err, documents) => {
					if(err){
						console.log(err);
					} else {
						documents  = filterDocuments(documents);
						let documentKind = connectionInfo.documentKinds[collection.id].documentKindName || '*';
						let documentTypes = [];

						if(documentKind !== '*'){
							documentTypes = documents.map(function(doc){
								return doc[documentKind];
							});
							documentTypes = documentTypes.filter((item) => Boolean(item));
							documentTypes = _.uniq(documentTypes);
						}

						let dataItem = prepareConnectionDataItem(documentTypes, collection.id, database);
						collItemCallback(err, dataItem);
					}
				});
			}
		});
	}, (err, items) => {
		if(err){
			console.log(err);
		}
		return dbItemCallback(err, items);
	});
}

function prepareConnectionDataItem(documentTypes, bucketName, database){
	let uniqueDocuments = _.uniq(documentTypes);
	let connectionDataItem = {
		dbName: bucketName,
		dbCollections: uniqueDocuments
	};

	return connectionDataItem;
}

function getSampleDocSize(count, recordSamplingSettings) {
	let per = recordSamplingSettings.relative.value;
	return (recordSamplingSettings.active === 'absolute')
		? recordSamplingSettings.absolute.value
			: Math.round( count/100 * per);
}

function getIndexes(indexingPolicy){
	let generalIndexes = [];
	
	if(indexingPolicy){
		indexingPolicy.includedPaths.forEach(item => {
			let indexes = item.indexes;
			indexes = indexes.map(index => {
				index.indexPrecision = index.precision;
				index.automatic = item.automatic;
				index.mode = indexingPolicy.indexingMode;
				index.indexIncludedPath = item.path;
				return index;
			});

			generalIndexes = generalIndexes.concat(generalIndexes, indexes);
		});
	}

	return generalIndexes;
}
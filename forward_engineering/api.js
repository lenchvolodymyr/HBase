module.exports = {
	generateScript(data, logger, cb) {
		const { jsonSchema, modelData, containerData, entityData, isUpdateScript } = data;
		let mappingScript = {
			mappings: {
				[entityData.collectionName.toLowerCase()]: {
					//properties: this.getMappingScript(JSON.parse(jsonSchema))
				}
			}
		};

		cb(null, mappingScript);
	}
};

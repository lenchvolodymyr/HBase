module.exports = {
	generateScript(data, logger, cb) {
		const { jsonSchema, modelData, containerData, entityData, isUpdateScript } = data;
		let columnFailies = this.getColumnFamilies(entityData);
		let script = `create '${entityData.collectionName.toLowerCase()}'`;
		
		columnFailies.forEach(item => {
			script = `${script}, '${item}'`;
		});

		cb(null, script);
	},

	getColumnFamilies(entity){
		return entity.properties.filter(item => {
			return item.type === 'colFam';
		}).map(item => item.name);
	}
};

module.exports = {
	generateScript(data, logger, cb) {
		let { jsonSchema, entityData } = data;

		try {
			jsonSchema = JSON.parse(jsonSchema);
		} catch (err) {
			return cb(err)
		}
		
		let columnFailies = this.getColumnFamilies(jsonSchema.properties);
		let script = `create '${entityData.collectionName.toLowerCase()}'`;
		
		columnFailies.forEach(item => {
			script = `${script}, '${item}'`;
		});

		return cb(null, script);
	},

	getColumnFamilies(props = {}){
		let columnFamilies = [];
		for(let prop in props){
			if(props[prop] && props[prop].type === 'colFam'){
				columnFamilies.push(prop);
			}
		}

		if(!columnFamilies.length){
			columnFamilies.push('<columnFamily>');
		}

		return columnFamilies;
	}
};

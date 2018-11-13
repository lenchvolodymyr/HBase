
const getTokenFromKerberos = (kerberos, service, user, password) => {
	const mechOID = kerberos.GSS_MECH_OID_SPNEGO;

	return kerberos.initializeClient(service, {
		user, password, mechOID
	}).then(client => {
		return client.step('');
	});
};

const getClient = kerberos => ({ principal: user, service_principal: service, password }) => Promise.resolve({
	token: (callback) => {
		getTokenFromKerberos(kerberos, service, user, password).then(
			(token) => callback(null, token),
			err => callback(err)
		);
	},
	destroy: (callback) => {
		callback();
	}
});

module.exports = ({ kerberos }) => {
	return {
		getClient: getClient(kerberos)
	};
};


const getTokenFromKerberos = (kerberos, service, principal, password) => {
	const mechOID = kerberos.GSS_MECH_OID_SPNEGO;
	const [ user, domain ] = principal.split('@');

	return kerberos.initializeClient(service, {
		user, domain, password, mechOID
	}).then(client => {
		return client.step('');
	});
};

const getClient = kerberos => ({ principal, service_principal: service, password }) => Promise.resolve({
	token: (callback) => {
		getTokenFromKerberos(kerberos, service, principal, password).then(
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

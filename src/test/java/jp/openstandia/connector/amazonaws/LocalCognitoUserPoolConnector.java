package jp.openstandia.connector.amazonaws;

public class LocalCognitoUserPoolConnector extends CognitoUserPoolConnector {
    @Override
    protected void authenticateResource() {
        client = MockClient.instance();
    }
}
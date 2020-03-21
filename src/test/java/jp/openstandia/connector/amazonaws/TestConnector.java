package jp.openstandia.connector.amazonaws;

public class TestConnector extends CognitoUserPoolConnector {
    @Override
    protected void authenticateResource() {
        client = MockClient.instance();
    }
}
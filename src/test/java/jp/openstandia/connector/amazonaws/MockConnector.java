package jp.openstandia.connector.amazonaws;

public class MockConnector extends CognitoUserPoolConnector {
    @Override
    protected void authenticateResource() {
        client = MockClient.instance();
    }
}
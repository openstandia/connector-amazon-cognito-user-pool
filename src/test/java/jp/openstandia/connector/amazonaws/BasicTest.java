package jp.openstandia.connector.amazonaws;

import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.ObjectClassInfo;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class BasicTest {

    ConnectorFacade connector;
    MockClient mockClient;

    private CognitoUserPoolConfiguration newConfiguration() {
        CognitoUserPoolConfiguration conf = new CognitoUserPoolConfiguration();
        conf.setUserPoolID("testPool");
        return conf;
    }

    private ConnectorFacade newFacade() {
        ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        APIConfiguration impl = TestHelpers.createTestConfiguration(MockConnector.class, newConfiguration());
        impl.getResultsHandlerConfiguration().setEnableAttributesToGetSearchResultsHandler(false);
        impl.getResultsHandlerConfiguration().setEnableNormalizingResultsHandler(false);
        impl.getResultsHandlerConfiguration().setEnableFilteredResultsHandler(false);
        return factory.newInstance(impl);
    }

    @BeforeEach
    void before() {
        connector = newFacade();
        mockClient = MockClient.instance();
        mockClient.init();
    }

    @Test
    void schema() {
        Schema schema = connector.schema();

        assertNotNull(schema);
        assertEquals(2, schema.getObjectClassInfo().size());

        Optional<ObjectClassInfo> user = schema.getObjectClassInfo().stream().filter(o -> o.is("User")).findFirst();
        Optional<ObjectClassInfo> group = schema.getObjectClassInfo().stream().filter(o -> o.is("Group")).findFirst();

        assertTrue(user.isPresent());
        assertTrue(group.isPresent());
    }

    @Test
    void test() {
        connector.test();
    }
}

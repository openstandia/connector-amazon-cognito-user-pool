package jp.openstandia.connector.amazonaws;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.*;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.cognitoidp.AWSCognitoIdentityProvider;
import com.amazonaws.services.cognitoidp.AWSCognitoIdentityProviderClientBuilder;
import com.amazonaws.services.cognitoidp.model.DescribeUserPoolRequest;
import com.amazonaws.services.cognitoidp.model.DescribeUserPoolResult;
import com.amazonaws.services.cognitoidp.model.UserPoolType;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.Connector;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.operations.*;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@ConnectorClass(configurationClass = CognitoUserPoolConfiguration.class, displayNameKey = "NRI OpenStandia Amazon Cognito User Pool Connector")
public class CognitoUserPoolConnector implements Connector, CreateOp, UpdateOp, DeleteOp, SchemaOp, TestOp, SearchOp<CognitoUserPoolFilter> {

    private static final Log LOG = Log.getLog(CognitoUserPoolConnector.class);

    private CognitoUserPoolConfiguration configuration;
    private AWSCognitoIdentityProvider client;

    private static Map<String, AttributeInfo> userSchemaMap;

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        this.configuration = (CognitoUserPoolConfiguration) configuration;
        authenticateResource();

        LOG.ok("Connector {0} successfully initialized", getClass().getName());
    }

    private void authenticateResource() {
        final AWSCredentialsProvider[] cp = {new DefaultAWSCredentialsProviderChain()};
        if (configuration.getAWSAccessKeyID() != null && configuration.getAWSSecretAccessKey() != null) {
            configuration.getAWSAccessKeyID().access(c -> {
                configuration.getAWSSecretAccessKey().access(s -> {
                    AWSCredentials cred = new BasicAWSCredentials(String.valueOf(c), String.valueOf(s));
                    cp[0] = new AWSStaticCredentialsProvider(cred);
                });
            });
        }

        AWSCognitoIdentityProviderClientBuilder builder = AWSCognitoIdentityProviderClientBuilder.standard()
                .withCredentials(cp[0]);

        String defaultRegion = configuration.getDefaultRegion();
        if (StringUtil.isNotEmpty(defaultRegion)) {
            try {
                Regions region = Regions.fromName(defaultRegion);
                builder.setRegion(region.getName());
            } catch (IllegalArgumentException e) {
                LOG.error(e, "Invalid default region: {0}", defaultRegion);
                throw new ConfigurationException("Invalid default region: " + defaultRegion);
            }
        }

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        if (StringUtil.isNotEmpty(configuration.getHttpProxyHost())) {
            clientConfiguration.setProxyProtocol(Protocol.HTTP);
            clientConfiguration.setProxyHost(configuration.getHttpProxyHost());
            clientConfiguration.setProxyPort(configuration.getHttpProxyPort());
            if (StringUtil.isNotEmpty(configuration.getHttpProxyUser())) {
                clientConfiguration.setProxyUsername(configuration.getHttpProxyUser());
                if (configuration.getHttpProxyPassword() != null) {
                    configuration.getHttpProxyPassword().access(c -> {
                        clientConfiguration.setProxyPassword(String.valueOf(c));
                    });
                }
            }
        }
        builder.setClientConfiguration(clientConfiguration);

        client = builder.build();

        // Verify we can access the user pool
        describeUserPool();
    }

    private UserPoolType describeUserPool() {
        DescribeUserPoolResult result = client.describeUserPool(new DescribeUserPoolRequest()
                .withUserPoolId(configuration.getUserPoolID()));
        int status = result.getSdkHttpMetadata().getHttpStatusCode();
        if (status != 200) {
            throw new ConnectorException("Failed to describe user pool: " + configuration.getUserPoolID());
        }
        return result.getUserPool();
    }

    @Override
    public synchronized Schema schema() {
        UserPoolType userPoolType = describeUserPool();

        SchemaBuilder schemaBuilder = new SchemaBuilder(CognitoUserPoolConnector.class);

        CognitoUserHandler usersHandler = new CognitoUserHandler(configuration, client);
        ObjectClassInfo userSchemaInfo = usersHandler.getUserSchema(userPoolType);
        schemaBuilder.defineObjectClass(userSchemaInfo);

        CognitoGroupHandler group = new CognitoGroupHandler(configuration, client);
        ObjectClassInfo groupSchemaInfo = group.getGroupSchema(userPoolType);
        schemaBuilder.defineObjectClass(groupSchemaInfo);

        userSchemaMap = new HashMap<>();
        userSchemaInfo.getAttributeInfo().stream()
                .forEach(a -> userSchemaMap.put(a.getName(), a));
        userSchemaMap.put(Uid.NAME, AttributeInfoBuilder.define("username").build());
        userSchemaMap = Collections.unmodifiableMap(userSchemaMap);

        return schemaBuilder.build();
    }

    private Map<String, AttributeInfo> getUserSchemaMap() {
        // Load schema map if it's not loaded yet
        if (userSchemaMap == null) {
            schema();
        }
        return userSchemaMap;
    }

    @Override
    public void dispose() {
        this.client = null;
    }

    @Override
    public Uid create(ObjectClass objectClass, Set<Attribute> createAttributes, OperationOptions operationOptions) {
        if (objectClass == null) {
            throw new InvalidAttributeValueException("ObjectClass value not provided");
        }
        LOG.info("CREATE METHOD OBJECTCLASS VALUE: {0}", objectClass);

        if (createAttributes == null) {
            throw new InvalidAttributeValueException("Attributes not provided or empty");
        }

        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            CognitoUserHandler usersHandler = new CognitoUserHandler(configuration, client);
            return usersHandler.createUser(getUserSchemaMap(), createAttributes);

        } else if (objectClass.is(ObjectClass.GROUP_NAME)) {
            CognitoGroupHandler groupsHandler = new CognitoGroupHandler(configuration, client);
            return groupsHandler.createGroup(createAttributes);

        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    @Override
    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> replaceAttributes, OperationOptions operationOptions) {
        if (objectClass == null) {
            throw new InvalidAttributeValueException("ObjectClass value not provided");
        }
        LOG.info("UPDATE METHOD OBJECTCLASS VALUE: {0}", objectClass);

        if (replaceAttributes == null) {
            throw new InvalidAttributeValueException("Attributes not provided or empty");
        }

        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            CognitoUserHandler usersHandler = new CognitoUserHandler(configuration, client);
            return usersHandler.updateUser(getUserSchemaMap(), objectClass, uid, replaceAttributes, operationOptions);

        } else if (objectClass.is(ObjectClass.GROUP_NAME)) {
            CognitoGroupHandler groupsHandler = new CognitoGroupHandler(configuration, client);
            return groupsHandler.updateGroup(objectClass, uid, replaceAttributes, operationOptions);
        }

        throw new UnsupportedOperationException("Unsupported object class " + objectClass);
    }

    @Override
    public void delete(ObjectClass objectClass, Uid uid, OperationOptions options) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            CognitoUserHandler usersHandler = new CognitoUserHandler(configuration, client);
            usersHandler.deleteUser(objectClass, uid, options);

        } else if (objectClass.is(ObjectClass.GROUP_NAME)) {
            CognitoGroupHandler groupsHandler = new CognitoGroupHandler(configuration, client);
            groupsHandler.deleteGroup(objectClass, uid, options);

        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    @Override
    public FilterTranslator<CognitoUserPoolFilter> createFilterTranslator(ObjectClass objectClass, OperationOptions operationOptions) {
        return new CognitoUserPoolFilterTranslator();
    }

    @Override
    public void executeQuery(ObjectClass objectClass, CognitoUserPoolFilter filter, ResultsHandler resultsHandler, OperationOptions operationOptions) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            CognitoUserHandler usersHandler = new CognitoUserHandler(configuration, client);
            usersHandler.getUsers(getUserSchemaMap(), filter, resultsHandler, operationOptions);

        } else if (objectClass.is(ObjectClass.GROUP_NAME)) {
            CognitoGroupHandler groupsHandler = new CognitoGroupHandler(configuration, client);
            groupsHandler.getGroups(filter, resultsHandler, operationOptions);

        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    @Override
    public void test() {
        dispose();
        authenticateResource();
    }
}

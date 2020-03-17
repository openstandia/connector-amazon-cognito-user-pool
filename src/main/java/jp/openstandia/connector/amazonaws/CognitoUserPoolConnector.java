package jp.openstandia.connector.amazonaws;

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.InstanceNameAware;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.*;
import software.amazon.awssdk.auth.credentials.*;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.apache.ProxyConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClientBuilder;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@ConnectorClass(configurationClass = CognitoUserPoolConfiguration.class, displayNameKey = "NRI OpenStandia Amazon Cognito User Pool Connector")
public class CognitoUserPoolConnector implements PoolableConnector, CreateOp, UpdateOp, DeleteOp, SchemaOp, TestOp, SearchOp<CognitoUserPoolFilter>, InstanceNameAware {

    private static final Log LOG = Log.getLog(CognitoUserPoolConnector.class);

    private CognitoUserPoolConfiguration configuration;
    private CognitoIdentityProviderClient client;

    private Map<String, AttributeInfo> userSchemaMap;
    private String instanceName;

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
        final AwsCredentialsProvider[] cp = {DefaultCredentialsProvider.create()};
        if (configuration.getAWSAccessKeyID() != null && configuration.getAWSSecretAccessKey() != null) {
            configuration.getAWSAccessKeyID().access(c -> {
                configuration.getAWSSecretAccessKey().access(s -> {
                    AwsCredentials cred = AwsBasicCredentials.create(String.valueOf(c), String.valueOf(s));
                    cp[0] = StaticCredentialsProvider.create(cred);
                });
            });
        }

        CognitoIdentityProviderClientBuilder builder = CognitoIdentityProviderClient.builder().credentialsProvider(cp[0]);

        String defaultRegion = configuration.getDefaultRegion();
        if (StringUtil.isNotEmpty(defaultRegion)) {
            try {
                Region region = Region.of(defaultRegion);
                builder.region(region);
            } catch (IllegalArgumentException e) {
                LOG.error(e, "Invalid default region: {0}", defaultRegion);
                throw new ConfigurationException("Invalid default region: " + defaultRegion);
            }
        }

        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder();

        if (StringUtil.isNotEmpty(configuration.getHttpProxyHost())) {
            ProxyConfiguration.Builder proxyBuilder = ProxyConfiguration.builder()
                    .endpoint(URI.create(String.format("http://%s:%d",
                            configuration.getHttpProxyHost(), configuration.getHttpProxyPort())));
            if (StringUtil.isNotEmpty(configuration.getHttpProxyUser()) && configuration.getHttpProxyPassword() != null) {
                configuration.getHttpProxyPassword().access(c -> {
                    proxyBuilder.username(configuration.getHttpProxyUser())
                            .password(String.valueOf(c));
                });
            }
            httpClientBuilder.proxyConfiguration(proxyBuilder.build());
        }

        client = builder.httpClientBuilder(httpClientBuilder).build();

        // Verify we can access the user pool
        describeUserPool();
    }

    private UserPoolType describeUserPool() {
        DescribeUserPoolResponse result = client.describeUserPool(DescribeUserPoolRequest.builder()
                .userPoolId(configuration.getUserPoolID()).build());
        int status = result.sdkHttpResponse().statusCode();
        if (status != 200) {
            throw new ConnectorException("Failed to describe user pool: " + configuration.getUserPoolID());
        }
        return result.userPool();
    }

    @Override
    public Schema schema() {
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
    public Uid create(ObjectClass objectClass, Set<Attribute> createAttributes, OperationOptions options) {
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
    public Uid update(ObjectClass objectClass, Uid uid, Set<Attribute> replaceAttributes, OperationOptions options) {
        if (objectClass == null) {
            throw new InvalidAttributeValueException("ObjectClass value not provided");
        }
        LOG.info("UPDATE METHOD OBJECTCLASS VALUE: {0}", objectClass);

        if (replaceAttributes == null) {
            throw new InvalidAttributeValueException("Attributes not provided or empty");
        }

        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            CognitoUserHandler usersHandler = new CognitoUserHandler(configuration, client);
            return usersHandler.updateUser(getUserSchemaMap(), objectClass, uid, replaceAttributes, options);

        } else if (objectClass.is(ObjectClass.GROUP_NAME)) {
            CognitoGroupHandler groupsHandler = new CognitoGroupHandler(configuration, client);
            return groupsHandler.updateGroup(objectClass, uid, replaceAttributes, options);
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
    public FilterTranslator<CognitoUserPoolFilter> createFilterTranslator(ObjectClass objectClass, OperationOptions options) {
        return new CognitoUserPoolFilterTranslator(objectClass, options);
    }

    @Override
    public void executeQuery(ObjectClass objectClass, CognitoUserPoolFilter filter, ResultsHandler resultsHandler, OperationOptions options) {
        if (objectClass.is(ObjectClass.ACCOUNT_NAME)) {
            try {
                CognitoUserHandler usersHandler = new CognitoUserHandler(configuration, client);
                usersHandler.getUsers(getUserSchemaMap(), filter, resultsHandler, options);
            } catch (UserNotFoundException e) {
                // Don't throw UnknownUidException
                return;
            }

        } else if (objectClass.is(ObjectClass.GROUP_NAME)) {
            try {
                CognitoGroupHandler groupsHandler = new CognitoGroupHandler(configuration, client);
                groupsHandler.getGroups(filter, resultsHandler, options);
            } catch (ResourceNotFoundException e) {
                // Don't throw UnknownUidException
                return;
            }

        } else {
            throw new UnsupportedOperationException("Unsupported object class " + objectClass);
        }
    }

    @Override
    public void test() {
        dispose();
        authenticateResource();
    }

    @Override
    public void dispose() {
        client.close();
        this.client = null;
    }

    @Override
    public void checkAlive() {
        // Do nothing
    }

    @Override
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }
}

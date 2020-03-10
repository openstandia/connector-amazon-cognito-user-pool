package jp.openstandia.connector.amazonaws;

import com.amazonaws.services.cognitoidp.AWSCognitoIdentityProvider;
import com.amazonaws.services.cognitoidp.model.*;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static jp.openstandia.connector.amazonaws.CognitoUtils.*;
import static jp.openstandia.connector.amazonaws.CognitoUtils.toAttributeType;

public class CognitoUserHandler {

    private static final Log LOGGER = Log.getLog(CognitoUserHandler.class);

    private static final String ATTR_SUB = "sub";

    // The username for the user. Must be unique within the user pool.
    // Must be a UTF-8 string between 1 and 128 characters. After the user is created, the username cannot be changed.
    // https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_AdminCreateUser.html
    private static final String ATTR_USERNAME = "username";

    // Standard Attributes
    // https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-settings-attributes.html
    private static final String ATTR_EMAIL = "email";
    private static final String ATTR_PREFERRED_USERNAME = "preferred_username";


    // Metadata
    private static final String ATTR_USER_CREATE_DATE = "UserCreateDate";
    private static final String ATTR_USER_LAST_MODIFIED_DATE = "UserLastModifiedDate";
    private static final String ATTR_USER_STATUS = "UserStatus";

    private static final CognitoUserPoolFilter.SubFilter SUB_FILTER = new CognitoUserPoolFilter.SubFilter();

    private final CognitoUserPoolConfiguration configuration;
    private final AWSCognitoIdentityProvider client;

    public CognitoUserHandler(CognitoUserPoolConfiguration configuration, AWSCognitoIdentityProvider client) {
        this.configuration = configuration;
        this.client = client;
    }

    public ObjectClassInfo getUserSchema(UserPoolType userPoolType) {
        LOGGER.ok("UserPoolType: {0}", userPoolType);

        ObjectClassInfoBuilder builder = new ObjectClassInfoBuilder();

        // sub (__UID__)
        builder.addAttributeInfo(
                AttributeInfoBuilder.define(Uid.NAME)
                        .setRequired(true)
                        .setCreateable(false)
                        .setUpdateable(false)
                        .setNativeName(ATTR_SUB)
                        .build()
        );

        // username (__NAME__)
        // Caution!! It is prohibited to update this value which is Amazon Cognito limitation.
        AttributeInfoBuilder usernameBuilder = AttributeInfoBuilder.define(Name.NAME)
                .setRequired(true)
                .setCreateable(true)
                .setUpdateable(false)
                .setNativeName(ATTR_USERNAME);
        Boolean caseSensitive = userPoolType.getUsernameConfiguration().isCaseSensitive();
        if (!caseSensitive) {
            usernameBuilder.setSubtype(AttributeInfo.Subtypes.STRING_CASE_IGNORE);
        }
        builder.addAttributeInfo(usernameBuilder.build());

        // Other attributes
        List<AttributeInfo> attrInfoList = userPoolType.getSchemaAttributes().stream()
                .filter(a -> !a.getName().equals(ATTR_SUB))
                .map(s -> {
                    AttributeInfoBuilder attrInfo = AttributeInfoBuilder.define(escapeName(s.getName()));
                    attrInfo.setRequired(s.isRequired());
                    attrInfo.setUpdateable(s.isMutable());

                    // https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_SchemaAttributeType.html#CognitoUserPools-Type-SchemaAttributeType-AttributeDataType
                    switch (s.getAttributeDataType()) {
                        case "String":
                            attrInfo.setType(String.class);
                            break;
                        case "Number":
                            attrInfo.setType(Integer.class);
                            break;
                        case "Boolean":
                            attrInfo.setType(Boolean.class);
                            break;
                        default:
                            attrInfo.setType(String.class);
                    }

                    if (s.getName().equals(ATTR_EMAIL) || s.getName().equals(ATTR_PREFERRED_USERNAME)) {
                        if (!caseSensitive) {
                            attrInfo.setSubtype(AttributeInfo.Subtypes.STRING_CASE_IGNORE);
                        }
                    }
                    return attrInfo.build();
                })
                .collect(Collectors.toList());

        // Metadata
        attrInfoList.add(AttributeInfoBuilder.define(ATTR_USER_CREATE_DATE)
                .setType(ZonedDateTime.class)
                .setCreateable(false)
                .setUpdateable(false)
                .setReturnedByDefault(true)
                .build());
        attrInfoList.add(AttributeInfoBuilder.define(ATTR_USER_LAST_MODIFIED_DATE)
                .setType(ZonedDateTime.class)
                .setCreateable(false)
                .setUpdateable(false)
                .setReturnedByDefault(true)
                .build());
        attrInfoList.add(AttributeInfoBuilder.define(ATTR_USER_STATUS)
                .setCreateable(false)
                .setUpdateable(false)
                .setReturnedByDefault(true)
                .build());

        builder.addAllAttributeInfo(attrInfoList);

        ObjectClassInfo userSchemaInfo = builder.build();

        LOGGER.ok("The constructed User core schema: {0}", userSchemaInfo);

        return userSchemaInfo;
    }


    public Uid createUser(Set<Attribute> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            throw new InvalidAttributeValueException("attributes not provided or empty");
        }

        AdminCreateUserRequest request = new AdminCreateUserRequest()
                .withUserPoolId(configuration.getUserPoolID());

        for (Attribute a : attributes) {
            if (a.getName().equals(Name.NAME)) {
                request.setUsername(AttributeUtil.getAsStringValue(a));
            } else {
                AttributeType attr = toAttributeType(a);
                request = request.withUserAttributes(attr);
            }
        }

        // Generate username if IDM doesn't have mapping to username
        if (request.getUsername() == null) {
            String uuid = UUID.randomUUID().toString();
            request.setUsername(uuid);
        }

        AdminCreateUserResult response = client.adminCreateUser(request);

        int status = response.getSdkHttpMetadata().getHttpStatusCode();
        if (status != 200) {
            throw new ConnectorException(String.format("Failed to create user. AdminCreateUser returned %d status. username: %s", status, request.getUsername()));
        }

        UserType user = response.getUser();
        return new Uid(user.getAttributes().stream().filter(a -> a.getName().equals(ATTR_SUB)).findFirst().get().getValue());
    }

    private Name resolveName(ObjectClass objectClass, Uid uid, OperationOptions options) {
        Name nameHint = uid.getNameHint();
        if (nameHint != null) {
            return nameHint;
        }

        UserType user = findUserByUid(uid.getUidValue(), null);
        if (user == null) {
            LOGGER.warn("Not found user while deleting. uid: {0}", uid);
            throw new UnknownUidException(uid, objectClass);
        }
        return new Name(user.getUsername());
    }


    public Uid updateUser(ObjectClass objectClass, Uid uid, Set<Attribute> replaceAttributes, OperationOptions options) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        Name name = resolveName(objectClass, uid, options);

        Collection<AttributeType> updateAttrs = new ArrayList<>();
//        Collection<String> deleteAttrs = new ArrayList<>();

        for (Attribute attr : replaceAttributes) {
            // When the IDM decided to delete the attribute, the value is null
            if (attr.getValue() == null) {
                updateAttrs.add(toAttributeTypeForDelete(attr));
            } else {
                updateAttrs.add(toAttributeType(attr));
            }
        }

        if (updateAttrs.size() > 0) {
            AdminUpdateUserAttributesRequest request = new AdminUpdateUserAttributesRequest()
                    .withUserPoolId(configuration.getUserPoolID())
                    .withUsername(name.getNameValue())
                    .withUserAttributes(updateAttrs);
            AdminUpdateUserAttributesResult result = client.adminUpdateUserAttributes(request);

            int status = result.getSdkHttpMetadata().getHttpStatusCode();
            if (status != 200) {
                throw new ConnectorException(String.format("Failed to update user. AdminUpdateUserAttributes returned %d status. uid: %s", status, uid));
            }
        }

        // We need to call another API to delete attributes.
        // It means that we can't execute this update as a single transaction.
        // Therefore, Cognito data may be inconsistent if below calling is failed.
        // Although this connector don't handle this situation, IDM can retry the update to resolve this inconsistency.
//        if (deleteAttrs.size() > 0) {
//            AdminDeleteUserAttributesRequest request = new AdminDeleteUserAttributesRequest()
//                    .withUserPoolId(configuration.getUserPoolID())
//                    .withUsername(name.getNameValue())
//                    .withUserAttributeNames(deleteAttrs);
//            AdminDeleteUserAttributesResult result = client.adminDeleteUserAttributes(request);
//
//            int status = result.getSdkHttpMetadata().getHttpStatusCode();
//            if (status != 200) {
//                throw new ConnectorException(String.format("Failed to update user. AdminDeleteUserAttributes returned %d status. uid: %s", status, uid));
//            }
//        }

        return uid;
    }

    public void deleteUser(ObjectClass objectClass, Uid uid, OperationOptions options) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        Name name = resolveName(objectClass, uid, options);

        AdminDeleteUserResult result = client.adminDeleteUser(new AdminDeleteUserRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withUsername(name.getNameValue()));

        int status = result.getSdkHttpMetadata().getHttpStatusCode();
        if (status != 200) {
            throw new ConnectorException(String.format("Failed to delete user. AdminDeleteUser returned %d status. uid: %s", status, uid));
        }
    }

    public UserType findUserByUid(String uid, OperationOptions operationOptions) {
        ListUsersResult result = client.listUsers(new ListUsersRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withFilter(SUB_FILTER.toFilterString(uid)));

        int status = result.getSdkHttpMetadata().getHttpStatusCode();
        if (status != 200) {
            throw new ConnectorException(String.format("Failed to get user by sub. ListUsers returned %d status. uid: %s", status, uid));
        }

        return result.getUsers().get(0);
    }

    public void getUserByName(Map<String, AttributeInfo> schema, String username, ResultsHandler resultsHandler, OperationOptions operationOptions) {
        AdminGetUserResult result = client.adminGetUser(new AdminGetUserRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withUsername(username));

        int status = result.getSdkHttpMetadata().getHttpStatusCode();
        if (status != 200) {
            throw new ConnectorException(String.format("Failed to get user by username. AdminGetUser returned %d status. username: %s", status, username));
        }

        resultsHandler.handle(toConnectorObject(schema, result));
    }

    public void getUsers(Map<String, AttributeInfo> schema, CognitoUserPoolFilter filter, ResultsHandler resultsHandler, OperationOptions options) {
        ListUsersRequest request = new ListUsersRequest();
        request.setUserPoolId(configuration.getUserPoolID());
        if (filter != null) {
            request.setFilter(filter.toFilterString(schema));
        }

        String paginationToken = null;

        do {
            request.setPaginationToken(paginationToken);

            ListUsersResult result = client.listUsers(request);

            result.getUsers().stream()
                    .forEach(u -> resultsHandler.handle(toConnectorObject(schema, u)));

            paginationToken = result.getPaginationToken();

        } while (paginationToken != null);
    }

    private ConnectorObject toConnectorObject(Map<String, AttributeInfo> schema, AdminGetUserResult result) {
        return toConnectorObject(schema, result.getUsername(), result.getEnabled(), result.getUserCreateDate(), result.getUserLastModifiedDate(),
                result.getUserStatus(), result.getUserAttributes());
    }

    private ConnectorObject toConnectorObject(Map<String, AttributeInfo> schema, UserType u) {
        return toConnectorObject(schema, u.getUsername(), u.getEnabled(), u.getUserCreateDate(), u.getUserLastModifiedDate(),
                u.getUserStatus(), u.getAttributes());
    }

    private ConnectorObject toConnectorObject(Map<String, AttributeInfo> schema, String username, boolean enabled,
                                              Date userCreateDate, Date userLastModifiedDate,
                                              String status, List<AttributeType> attributes) {
        final ConnectorObjectBuilder builder = new ConnectorObjectBuilder()
                .setName(username)
                // Metadata
                .addAttribute(AttributeBuilder.buildEnabled(enabled))
                .addAttribute(ATTR_USER_CREATE_DATE, CognitoUtils.toZoneDateTime(userCreateDate))
                .addAttribute(ATTR_USER_LAST_MODIFIED_DATE, CognitoUtils.toZoneDateTime(userLastModifiedDate))
                .addAttribute(ATTR_USER_STATUS, status);

        for (AttributeType a : attributes) {
            if (a.getName().equals(ATTR_SUB)) {
                builder.setUid(a.getValue());
            } else {
                AttributeInfo attributeInfo = schema.get(escapeName(a.getName()));
                builder.addAttribute(toAttribute(attributeInfo, a));
            }
        }

        return builder.build();
    }

    private Attribute toAttribute(AttributeInfo attributeInfo, AttributeType a) {
        if (attributeInfo.getType() == Integer.class) {
            return AttributeBuilder.build(escapeName(a.getName()), Integer.parseInt(a.getValue()));
        }
        if (attributeInfo.getType() == Boolean.class) {
            return AttributeBuilder.build(escapeName(a.getName()), Boolean.parseBoolean(a.getValue()));
        }

        // String
        return AttributeBuilder.build(escapeName(a.getName()), a.getValue());
    }
}

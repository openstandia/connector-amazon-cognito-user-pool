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

public class CognitoUserHandler {

    private static final Log LOGGER = Log.getLog(CognitoUserHandler.class);

    // The username for the user. Must be unique within the user pool.
    // Must be a UTF-8 string between 1 and 128 characters. After the user is created, the username cannot be changed.
    // https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_AdminCreateUser.html
    private static final String ATTR_USERNAME = "username";

    // Also, Unique and unchangeable within the user pool
    private static final String ATTR_SUB = "sub";

    // Standard Attributes
    // https://docs.aws.amazon.com/cognito/latest/developerguide/user-pool-settings-attributes.html
    private static final String ATTR_EMAIL = "email";
    private static final String ATTR_PREFERRED_USERNAME = "preferred_username";

    // Metadata
    private static final String ATTR_USER_CREATE_DATE = "UserCreateDate";
    private static final String ATTR_USER_LAST_MODIFIED_DATE = "UserLastModifiedDate";
    private static final String ATTR_USER_STATUS = "UserStatus";

    // Association
    private static final String ATTR_GROUPS = "groups";

    private static final CognitoUserPoolFilter.SubFilter SUB_FILTER = new CognitoUserPoolFilter.SubFilter();

    private final CognitoUserPoolConfiguration configuration;
    private final AWSCognitoIdentityProvider client;
    private final CognitoUserGroupHandler userGroupHandler;

    public CognitoUserHandler(CognitoUserPoolConfiguration configuration, AWSCognitoIdentityProvider client) {
        this.configuration = configuration;
        this.client = client;
        this.userGroupHandler = new CognitoUserGroupHandler(configuration, client);
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

        // __ENABLE__ attribute
        builder.addAttributeInfo(OperationalAttributeInfos.ENABLE);

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
                        case "DateTime":
                            attrInfo.setType(ZonedDateTime.class);
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

        // Association
        attrInfoList.add(AttributeInfoBuilder.define(ATTR_GROUPS)
                .setCreateable(true)
                .setUpdateable(true)
                .setReturnedByDefault(false)
                .setMultiValued(true)
                .build());

        builder.addAllAttributeInfo(attrInfoList);

        ObjectClassInfo userSchemaInfo = builder.build();

        LOGGER.ok("The constructed User core schema: {0}", userSchemaInfo);

        return userSchemaInfo;
    }

    /**
     * The spec for AdminCreateUser:
     * https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_AdminCreateUser.html
     *
     * @param schema
     * @param attributes
     * @return
     */
    public Uid createUser(Map<String, AttributeInfo> schema, Set<Attribute> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            throw new InvalidAttributeValueException("attributes not provided or empty");
        }

        AdminCreateUserRequest request = new AdminCreateUserRequest()
                .withUserPoolId(configuration.getUserPoolID());

        boolean userEnabled = true;
        List<Object> groups = null;

        for (Attribute a : attributes) {
            if (a.getName().equals(Name.NAME)) {
                request.setUsername(AttributeUtil.getAsStringValue(a));
            } else if (a.getName().equals(OperationalAttributes.ENABLE_NAME)) {
                userEnabled = AttributeUtil.getBooleanValue(a);
            } else if (a.getName().equals(ATTR_GROUPS)) {
                groups = a.getValue();
            } else {
                AttributeType attr = toCognitoAttribute(schema, a);
                request = request.withUserAttributes(attr);
            }
        }

        // Generate username if IDM doesn't have mapping to username
        if (request.getUsername() == null) {
            String uuid = UUID.randomUUID().toString();
            request.setUsername(uuid);
        }

        AdminCreateUserResult result = client.adminCreateUser(request);

        checkCognitoResult(result, "AdminCreateUser");

        UserType user = result.getUser();
        Uid newUid = new Uid(user.getAttributes().stream()
                .filter(a -> a.getName().equals(ATTR_SUB)).findFirst().get().getValue(), user.getUsername());

        // We need to call another API to enable/disable user.
        // It means that we can't execute this operation as a single transaction.
        // Therefore, Cognito data may be inconsistent if below calling is failed.
        // Although this connector don't handle this situation, IDM can retry the update to resolve this inconsistency.
        if (!userEnabled) {
            disableUser(newUid, newUid.getNameHint());
        }

        // We need to call another API to add/remove group for this user.
        // It means that we can't execute this update as a single transaction.
        // Therefore, Cognito data may be inconsistent if below calling is failed.
        // Although this connector doesn't handle this situation, IDM can retry the update to resolve this inconsistency.
        userGroupHandler.updateGroupsToUser(newUid.getNameHint(), groups);

        return newUid;
    }

    /**
     * The spec for AdminUpdateUserAttributes:
     * https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_AdminUpdateUserAttributes.html
     *
     * @param schema
     * @param objectClass
     * @param uid
     * @param replaceAttributes
     * @param options
     * @return
     */
    public Uid updateUser(Map<String, AttributeInfo> schema, ObjectClass objectClass,
                          Uid uid, Set<Attribute> replaceAttributes, OperationOptions options) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        Name name = resolveName(objectClass, uid, options);

        Collection<AttributeType> updateAttrs = new ArrayList<>();
//        Collection<String> deleteAttrs = new ArrayList<>();

        Boolean userEnabled = null;
        List<Object> groups = null;

        for (Attribute attr : replaceAttributes) {
            // When the IDM decided to delete the attribute, the value is null
            if (attr.getValue() == null) {
                updateAttrs.add(toCognitoAttributeForDelete(attr));
            } else if (attr.getName().equals(OperationalAttributes.ENABLE_NAME)) {
                userEnabled = AttributeUtil.getBooleanValue(attr);
            } else if (attr.getName().equals(ATTR_GROUPS)) {
                groups = attr.getValue();
            } else {
                updateAttrs.add(toCognitoAttribute(schema, attr));
            }
        }

        if (updateAttrs.size() > 0) {
            AdminUpdateUserAttributesRequest request = new AdminUpdateUserAttributesRequest()
                    .withUserPoolId(configuration.getUserPoolID())
                    .withUsername(name.getNameValue())
                    .withUserAttributes(updateAttrs);
            try {
                AdminUpdateUserAttributesResult result = client.adminUpdateUserAttributes(request);

                checkCognitoResult(result, "AdminUpdateUserAttributes");
            } catch (UserNotFoundException e) {
                LOGGER.warn("Not found user when deleting. uid: {0}", uid);
                throw new UnknownUidException(uid, objectClass);
            }
        }

        // We need to call another API to delete attributes.
        // It means that we can't execute this update as a single transaction.
        // Therefore, Cognito data may be inconsistent if below calling is failed.
        // Although this connector doesn't handle this situation, IDM can retry the update to resolve this inconsistency.
//        if (deleteAttrs.size() > 0) {
//            AdminDeleteUserAttributesRequest request = new AdminDeleteUserAttributesRequest()
//                    .withUserPoolId(configuration.getUserPoolID())
//                    .withUsername(name.getNameValue())
//                    .withUserAttributeNames(deleteAttrs);
//            AdminDeleteUserAttributesResult result = client.adminDeleteUserAttributes(request);
//
//            checkCognitoResult(result, "AdminDeleteUserAttributes");
//        }

        // We need to call another API to enable/disable user.
        // It means that we can't execute this update as a single transaction.
        // Therefore, Cognito data may be inconsistent if below calling is failed.
        // Although this connector doesn't handle this situation, IDM can retry the update to resolve this inconsistency.
        enableOrDisableUser(uid, name, userEnabled);

        // We need to call another API to add/remove group for this user.
        // It means that we can't execute this update as a single transaction.
        // Therefore, Cognito data may be inconsistent if below calling is failed.
        // Although this connector doesn't handle this situation, IDM can retry the update to resolve this inconsistency.
        userGroupHandler.updateGroupsToUser(name, groups);

        return uid;
    }

    private void enableOrDisableUser(Uid uid, Name name, Boolean userEnabled) {
        if (userEnabled != null) {
            if (userEnabled) {
                enableUser(uid, name);
            } else {
                disableUser(uid, name);
            }
        }
    }

    private void enableUser(Uid uid, Name name) {
        AdminEnableUserRequest request = new AdminEnableUserRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withUsername(name.getNameValue());
        try {
            AdminEnableUserResult result = client.adminEnableUser(request);

            checkCognitoResult(result, "AdminEnableUser");
        } catch (UserNotFoundException e) {
            LOGGER.warn("Not found user when enabling. uid: {0}", uid);
            throw new UnknownUidException(uid, ObjectClass.ACCOUNT);
        }
    }

    private void disableUser(Uid uid, Name name) {
        AdminDisableUserRequest request = new AdminDisableUserRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withUsername(name.getNameValue());
        try {
            AdminDisableUserResult result = client.adminDisableUser(request);

            checkCognitoResult(result, "AdminDisableUser");
        } catch (UserNotFoundException e) {
            LOGGER.warn("Not found user when disabling. uid: {0}", uid);
            throw new UnknownUidException(uid, ObjectClass.ACCOUNT);
        }
    }

    /**
     * The spec for AdminDeleteUser:
     * https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_AdminDeleteUser.html
     *
     * @param objectClass
     * @param uid
     * @param options
     */
    public void deleteUser(ObjectClass objectClass, Uid uid, OperationOptions options) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        Name name = resolveName(objectClass, uid, options);

        try {
            AdminDeleteUserResult result = client.adminDeleteUser(new AdminDeleteUserRequest()
                    .withUserPoolId(configuration.getUserPoolID())
                    .withUsername(name.getNameValue()));

            checkCognitoResult(result, "AdminDeleteUser");
        } catch (UserNotFoundException e) {
            LOGGER.warn("Not found user when deleting. uid: {0}", uid);
            throw new UnknownUidException(uid, objectClass);
        }
    }

    private Name resolveName(ObjectClass objectClass, Uid uid, OperationOptions options) {
        Name nameHint = uid.getNameHint();
        if (nameHint != null) {
            return nameHint;
        }

        // Fallback
        // If uid doesn't have Name hint, find the user by uid(sub)
        UserType user = findUserByUid(uid.getUidValue());
        if (user == null) {
            LOGGER.warn("Not found user when updating or deleting. uid: {0}", uid);
            throw new UnknownUidException(uid, objectClass);
        }
        return new Name(user.getUsername());
    }

    private UserType findUserByUid(String uid) {
        ListUsersResult result = client.listUsers(new ListUsersRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withFilter(SUB_FILTER.toFilterString(uid)));

        checkCognitoResult(result, "ListUsers");

        // ListUsers returns empty users list if no hits
        List<UserType> users = result.getUsers();
        if (users.isEmpty()) {
            return null;
        }

        if (users.size() > 1) {
            throw new ConnectorException(String.format("Unexpected error. ListUsers returns multiple users when searching by sub = \"%s\"", uid));
        }

        return result.getUsers().get(0);
    }

    private AdminGetUserResult findUserByName(String username) {
        AdminGetUserResult result = client.adminGetUser(new AdminGetUserRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withUsername(username));

        checkCognitoResult(result, "AdminGetUser");

        return result;
    }

    public void getUsers(Map<String, AttributeInfo> schema, CognitoUserPoolFilter filter, ResultsHandler resultsHandler, OperationOptions options) {
        if (filter != null && filter.isByName()) {
            getUserByName(schema, filter.attributeValue, resultsHandler, options);
            return;
        }

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
                    .forEach(u -> resultsHandler.handle(toConnectorObject(schema, u, options)));

            paginationToken = result.getPaginationToken();

        } while (paginationToken != null);
    }

    private void getUserByName(Map<String, AttributeInfo> schema, String username, ResultsHandler resultsHandler, OperationOptions options) {
        String[] attributesToGet = options.getAttributesToGet();
        if (attributesToGet == null) {
            AdminGetUserResult result = findUserByName(username);
            resultsHandler.handle(toConnectorObject(schema, result, options));
            return;
        }

        final ConnectorObjectBuilder builder = new ConnectorObjectBuilder()
                .setName(username);

        boolean loadFull = false;
        for (String getAttr : attributesToGet) {
            switch (getAttr) {
                case ATTR_GROUPS:
                    if (options.getAllowPartialAttributeValues() != Boolean.TRUE) {
                        List<String> groups = userGroupHandler.getGroupsForUser(username);
                        builder.addAttribute(ATTR_GROUPS, groups);
                    }
                default:
                    loadFull = true;
            }
        }

        if (loadFull) {
            AdminGetUserResult result = findUserByName(username);

            builder.addAttribute(AttributeBuilder.buildEnabled(result.getEnabled()))
                    .addAttribute(ATTR_USER_CREATE_DATE, CognitoUtils.toZoneDateTime(result.getUserCreateDate()))
                    .addAttribute(ATTR_USER_LAST_MODIFIED_DATE, CognitoUtils.toZoneDateTime(result.getUserLastModifiedDate()))
                    .addAttribute(ATTR_USER_STATUS, result.getUserStatus());

            for (AttributeType a : result.getUserAttributes()) {
                if (a.getName().equals(ATTR_SUB)) {
                    builder.setUid(a.getValue());
                } else {
                    AttributeInfo attributeInfo = schema.get(escapeName(a.getName()));
                    builder.addAttribute(toConnectorAttribute(attributeInfo, a));
                }
            }
        }

        resultsHandler.handle(builder.build());
    }

    private ConnectorObject toConnectorObject(Map<String, AttributeInfo> schema, AdminGetUserResult result, OperationOptions options) {
        return toConnectorObject(schema, result.getUsername(), result.getEnabled(), result.getUserCreateDate(), result.getUserLastModifiedDate(),
                result.getUserStatus(), result.getUserAttributes(), options);
    }

    private ConnectorObject toConnectorObject(Map<String, AttributeInfo> schema, UserType u, OperationOptions options) {
        return toConnectorObject(schema, u.getUsername(), u.getEnabled(), u.getUserCreateDate(), u.getUserLastModifiedDate(),
                u.getUserStatus(), u.getAttributes(), options);
    }

    private ConnectorObject toConnectorObject(Map<String, AttributeInfo> schema, String username, boolean enabled,
                                              Date userCreateDate, Date userLastModifiedDate,
                                              String status, List<AttributeType> attributes, OperationOptions options) {
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
                builder.addAttribute(toConnectorAttribute(attributeInfo, a));
            }
        }

        if (options.getAllowPartialAttributeValues() != Boolean.TRUE) {
            List<String> groups = userGroupHandler.getGroupsForUser(username);
            builder.addAttribute(ATTR_GROUPS, groups);
        } else {
            AttributeBuilder ab = new AttributeBuilder();
            ab.setName(ATTR_GROUPS).setAttributeValueCompleteness(AttributeValueCompleteness.INCOMPLETE);
            builder.addAttribute(ab.build());
        }

        return builder.build();
    }

    private Attribute toConnectorAttribute(AttributeInfo attributeInfo, AttributeType a) {
        // Cognito API returns the attribute as string even if it's other types.
        // We need to check the type from the schema and convert it.
        // Also, we must escape the name for custom attributes (The name of custom attribute starts with "custom:").
        if (attributeInfo.getType() == Integer.class) {
            return AttributeBuilder.build(escapeName(a.getName()), Integer.parseInt(a.getValue()));
        }
        if (attributeInfo.getType() == ZonedDateTime.class) {
            // The format is YYYY-MM-DD
            return AttributeBuilder.build(escapeName(a.getName()), toZoneDateTime(a.getValue()));
        }
        if (attributeInfo.getType() == Boolean.class) {
            return AttributeBuilder.build(escapeName(a.getName()), Boolean.parseBoolean(a.getValue()));
        }

        // String
        return AttributeBuilder.build(escapeName(a.getName()), a.getValue());
    }
}


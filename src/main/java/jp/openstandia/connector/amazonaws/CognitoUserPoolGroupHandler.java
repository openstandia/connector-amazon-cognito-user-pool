package jp.openstandia.connector.amazonaws;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.exceptions.RetryableException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;
import software.amazon.awssdk.services.cognitoidentityprovider.paginators.ListGroupsIterable;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

import static jp.openstandia.connector.amazonaws.CognitoUserPoolUtils.checkCognitoResult;
import static jp.openstandia.connector.amazonaws.CognitoUserPoolUtils.toZoneDateTime;

public class CognitoUserPoolGroupHandler {

    public static final ObjectClass GROUP_OBJECT_CLASS = new ObjectClass("Group");

    private static final Log LOGGER = Log.getLog(CognitoUserPoolGroupHandler.class);

    // Unique and unchangeable within the user pool
    private static final String ATTR_GROUP_NAME = "GroupName";

    // Attributes
    private static final String ATTR_DESCRIPTION = "Description";
    private static final String ATTR_PRECEDENCE = "Precedence";
    private static final String ATTR_ROLE_ARN = "RoleArn";

    // Metadata
    private static final String ATTR_CREATION_DATE = "CreationDate";
    private static final String ATTR_LAST_MODIFIED_DATE = "LastModifiedDate";

    // Association
    private static final String ATTR_USERS = "users";

    private final CognitoUserPoolConfiguration configuration;
    private final CognitoIdentityProviderClient client;
    private final CognitoUserPoolAssociationHandler userGroupHandler;

    public CognitoUserPoolGroupHandler(CognitoUserPoolConfiguration configuration, CognitoIdentityProviderClient client) {
        this.configuration = configuration;
        this.client = client;
        this.userGroupHandler = new CognitoUserPoolAssociationHandler(configuration, client);
    }

    public static ObjectClassInfo getGroupSchema(UserPoolType userPoolType) {
        ObjectClassInfoBuilder builder = new ObjectClassInfoBuilder();
        builder.setType(GROUP_OBJECT_CLASS.getObjectClassValue());

        // __UID__
        builder.addAttributeInfo(AttributeInfoBuilder.define(Uid.NAME)
                .setRequired(true)
                .setUpdateable(false)
                .setNativeName(ATTR_GROUP_NAME)
                .build());
        // __NAME__
        builder.addAttributeInfo(AttributeInfoBuilder.define(Name.NAME)
                .setRequired(true)
                .setUpdateable(false)
                .setNativeName(ATTR_GROUP_NAME)
                .build());

        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_CREATION_DATE)
                .setType(ZonedDateTime.class)
                .setCreateable(false)
                .setUpdateable(false)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_LAST_MODIFIED_DATE)
                .setType(ZonedDateTime.class)
                .setCreateable(false)
                .setUpdateable(false)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_DESCRIPTION)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_PRECEDENCE)
                .setType(Integer.class)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_ROLE_ARN)
                .build());

        // Association
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_USERS)
                .setMultiValued(true)
                .setReturnedByDefault(false)
                .build());

        ObjectClassInfo groupSchemaInfo = builder.build();

        LOGGER.info("The constructed Group core schema: {0}", groupSchemaInfo);

        return groupSchemaInfo;
    }

    /**
     * The spec for CreateGroup:
     * https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_CreateGroup.html
     *
     * @param attributes
     * @return
     * @throws AlreadyExistsException Object with the specified _NAME_ already exists.
     *                                Or there is a similar violation in any of the object attributes that
     *                                cannot be distinguished from AlreadyExists situation.
     */
    public Uid createGroup(Set<Attribute> attributes) throws AlreadyExistsException {
        if (attributes == null || attributes.isEmpty()) {
            throw new InvalidAttributeValueException("attributes not provided or empty");
        }

        CreateGroupRequest.Builder builder = CreateGroupRequest.builder()
                .userPoolId(configuration.getUserPoolID());

        List<Object> users = null;

        for (Attribute a : attributes) {
            users = buildCreateRequest(builder, a);
        }

        CreateGroupRequest request = builder.build();

        CreateGroupResponse result = null;
        try {
            result = client.createGroup(request);

            checkCognitoResult(result, "CreateGroup");
        } catch (GroupExistsException e) {
            LOGGER.warn(e, "The group already exists when creating. uid: {0}", request.groupName());
            throw new AlreadyExistsException("The group exists. GroupName: " + request.groupName(), e);
        }

        GroupType group = result.group();
        Uid newUid = new Uid(group.groupName(), group.groupName());

        try {
            // We need to call another API to add/remove user for this group.
            // It means that we can't execute this update as a single transaction.
            // Therefore, Cognito data may be inconsistent if below calling is failed.
            // Although this connector doesn't handle this situation, IDM can retry the update to resolve this inconsistency.
            userGroupHandler.updateUsersToGroup(newUid, users);

        } catch (ResourceNotFoundException e) {
            LOGGER.warn(e, "The group was deleted when setting users of the group after created. GroupName: {0}", request.groupName());
            throw RetryableException.wrap("The group was deleted when setting users of the group after created. GroupName: "
                    + request.groupName(), e);
        } catch (UserNotFoundException e) {
            LOGGER.warn(e, "The user was deleted when setting users of the group after created. GroupName: {0}", request.groupName());
            throw RetryableException.wrap("The user was deleted when setting users the group after created. GroupName: "
                    + request.groupName(), e);
        }

        return newUid;
    }

    /**
     * The spec for UpdateGroup:
     * https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_UpdateGroup.html
     *
     * @param uid
     * @param replaceAttributes
     * @param operationOptions
     * @return
     */
    public Uid updateGroup(Uid uid, Set<Attribute> replaceAttributes, OperationOptions operationOptions) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        UpdateGroupRequest.Builder request = UpdateGroupRequest.builder()
                .userPoolId(configuration.getUserPoolID())
                .groupName(uid.getUidValue());

        List<Object> users = null;

        for (Attribute a : replaceAttributes) {
            if (a.getValue() == null) {
                users = buildDeleteValue(request, a);
            } else {
                users = buildReplaceValue(request, a);
            }
        }
        UpdateGroupRequest req = request.build();

        if (req.description() != null ||
                req.precedence() != null ||
                req.roleArn() != null) {
            try {
                UpdateGroupResponse result = client.updateGroup(req);

                checkCognitoResult(result, "UpdateGroup");
            } catch (ResourceNotFoundException e) {
                LOGGER.warn("Not found group when updating. uid: {0}", uid);
                throw new UnknownUidException(uid, GROUP_OBJECT_CLASS);
            }
        }

        // We need to call another API to add/remove user for this group.
        // It means that we can't execute this update as a single transaction.
        // Therefore, Cognito data may be inconsistent if below calling is failed.
        // Although this connector doesn't handle this situation, IDM can retry the update to resolve this inconsistency.
        try {
            userGroupHandler.updateUsersToGroup(uid, users);
        } catch (ResourceNotFoundException e) {
            LOGGER.warn(e, "Not found group when updating. uid: {0}", uid);
            throw new UnknownUidException(uid, GROUP_OBJECT_CLASS);
        } catch (UserNotFoundException e) {
            LOGGER.warn(e, "Not found the user when updating. uid: {0}, users: {1}", uid, users);
            throw RetryableException.wrap("Need to retry because the user was deleted", e);
        }

        return uid;
    }

    private List<Object> buildCreateRequest(CreateGroupRequest.Builder builder, Attribute a) {
        List<Object> users = null;
        switch (a.getName()) {
            case "__UID__":
            case "__NAME__":
                builder.groupName(AttributeUtil.getAsStringValue(a));
            case ATTR_DESCRIPTION:
                builder.description(AttributeUtil.getAsStringValue(a));
                break;
            case ATTR_PRECEDENCE:
                builder.precedence(AttributeUtil.getIntegerValue(a));
                break;
            case ATTR_ROLE_ARN:
                builder.roleArn(AttributeUtil.getAsStringValue(a));
                break;
            case ATTR_USERS:
                users = a.getValue();
                break;
        }
        return users;
    }

    private List<Object> buildReplaceValue(UpdateGroupRequest.Builder request, Attribute a) {
        List<Object> users = null;
        switch (a.getName()) {
            case ATTR_DESCRIPTION:
                request.description(AttributeUtil.getAsStringValue(a));
                break;
            case ATTR_PRECEDENCE:
                request.precedence(AttributeUtil.getIntegerValue(a));
                break;
            case ATTR_ROLE_ARN:
                request.roleArn(AttributeUtil.getAsStringValue(a));
                break;
            case ATTR_USERS:
                users = a.getValue();
                break;
        }
        return users;
    }

    private List<Object> buildDeleteValue(UpdateGroupRequest.Builder request, Attribute a) {
        List<Object> users = null;
        switch (a.getName()) {
            case ATTR_DESCRIPTION:
                // Description is removed if we set ""
                request.description("");
                break;
            case ATTR_PRECEDENCE:
                // Precedence is removed if we set 0
                request.precedence(0);
                break;
            case ATTR_ROLE_ARN:
                // RoleArn is removed if we set ""
                request.roleArn("");
                break;
            case ATTR_USERS:
                users = a.getValue();
                break;
        }
        return users;
    }

    /**
     * The spec for DeleteGroup:
     * https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_DeleteGroup.html
     *
     * @param objectClass
     * @param uid
     * @param options
     */
    public void deleteGroup(ObjectClass objectClass, Uid uid, OperationOptions options) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        try {
            userGroupHandler.removeAllUsers(uid.getUidValue());

            DeleteGroupResponse result = client.deleteGroup(DeleteGroupRequest.builder()
                    .userPoolId(configuration.getUserPoolID())
                    .groupName(uid.getUidValue()).build());

            checkCognitoResult(result, "DeleteGroup");
        } catch (ResourceNotFoundException e) {
            LOGGER.warn("Not found group when deleting. uid: {0}", uid);
            throw new UnknownUidException(uid, objectClass);
        }
    }

    /**
     * The spec for ListGroups:
     * https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_ListGroups.html
     *
     * @param filter
     * @param resultsHandler
     * @param options
     */
    public void getGroups(CognitoUserPoolFilter filter,
                          ResultsHandler resultsHandler, OperationOptions options) {
        if (filter != null && (filter.isByName() || filter.isByUid())) {
            getGroupByName(filter.attributeValue, resultsHandler, options);
            return;
        }

        // Cannot filter using Cognito API unfortunately...
        // So we always return all groups here.
        ListGroupsRequest.Builder request = ListGroupsRequest.builder();
        request.userPoolId(configuration.getUserPoolID());

        ListGroupsIterable result = client.listGroupsPaginator(request.build());

        result.forEach(r -> r.groups().forEach(g -> resultsHandler.handle(toConnectorObject(g, options))));
    }

    private void getGroupByName(String groupName,
                                ResultsHandler resultsHandler, OperationOptions options) {
        GetGroupResponse result = client.getGroup(GetGroupRequest.builder()
                .userPoolId(configuration.getUserPoolID())
                .groupName(groupName).build());

        checkCognitoResult(result, "GetGroup");

        resultsHandler.handle(toConnectorObject(result.group(), options));
    }

    private ConnectorObject toConnectorObject(GroupType g, OperationOptions options) {
        String[] attributesToGet = options.getAttributesToGet();
        if (attributesToGet == null) {
            return toFullConnectorObject(g);
        }

        ConnectorObjectBuilder builder = new ConnectorObjectBuilder()
                .setObjectClass(GROUP_OBJECT_CLASS)
                .setUid(g.groupName())
                .setName(g.groupName());

        for (String getAttr : attributesToGet) {
            switch (getAttr) {
                case ATTR_DESCRIPTION:
                    builder.addAttribute(ATTR_DESCRIPTION, g.description());
                case ATTR_PRECEDENCE:
                    builder.addAttribute(ATTR_PRECEDENCE, g.precedence());
                case ATTR_ROLE_ARN:
                    builder.addAttribute(ATTR_ROLE_ARN, g.roleArn());
                case ATTR_CREATION_DATE:
                    builder.addAttribute(ATTR_CREATION_DATE, toZoneDateTime(g.creationDate()));
                case ATTR_LAST_MODIFIED_DATE:
                    builder.addAttribute(ATTR_LAST_MODIFIED_DATE, toZoneDateTime(g.lastModifiedDate()));
                case ATTR_USERS:
                    builder.addAttribute(ATTR_USERS, userGroupHandler.getUsersInGroup(g.groupName()));
            }
        }

        return builder.build();
    }

    private ConnectorObject toFullConnectorObject(GroupType g) {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder()
                .setObjectClass(GROUP_OBJECT_CLASS)
                .setUid(new Uid(g.groupName(), g.groupName()))
                .setName(g.groupName())
                .addAttribute(ATTR_DESCRIPTION, g.description())
                .addAttribute(ATTR_PRECEDENCE, g.precedence())
                .addAttribute(ATTR_ROLE_ARN, g.roleArn())
                .addAttribute(ATTR_CREATION_DATE, toZoneDateTime(g.creationDate()))
                .addAttribute(ATTR_LAST_MODIFIED_DATE, toZoneDateTime(g.lastModifiedDate()));

        return builder.build();
    }
}

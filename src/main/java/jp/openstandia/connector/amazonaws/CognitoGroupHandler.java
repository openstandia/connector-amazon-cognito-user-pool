package jp.openstandia.connector.amazonaws;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;
import software.amazon.awssdk.services.cognitoidentityprovider.paginators.ListGroupsIterable;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;

import static jp.openstandia.connector.amazonaws.CognitoUtils.checkCognitoResult;
import static jp.openstandia.connector.amazonaws.CognitoUtils.toZoneDateTime;

public class CognitoGroupHandler {

    private static final Log LOGGER = Log.getLog(CognitoGroupHandler.class);

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
    private final CognitoUserGroupHandler userGroupHandler;

    public CognitoGroupHandler(CognitoUserPoolConfiguration configuration, CognitoIdentityProviderClient client) {
        this.configuration = configuration;
        this.client = client;
        this.userGroupHandler = new CognitoUserGroupHandler(configuration, client);
    }

    public ObjectClassInfo getGroupSchema(UserPoolType userPoolType) {
        ObjectClassInfoBuilder builder = new ObjectClassInfoBuilder();
        builder.setType(ObjectClass.GROUP_NAME);

        // __UID__
        builder.addAttributeInfo(AttributeInfoBuilder.define(Uid.NAME)
                .setRequired(true)
                .setCreateable(true)
                .setUpdateable(false)
                .setReturnedByDefault(true)
                .setNativeName(ATTR_GROUP_NAME)
                .build());
        // __NAME__
        builder.addAttributeInfo(AttributeInfoBuilder.define(Name.NAME)
                .setRequired(true)
                .setCreateable(true)
                .setUpdateable(false)
                .setReturnedByDefault(true)
                .setNativeName(ATTR_GROUP_NAME)
                .build());

        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_CREATION_DATE)
                .setType(ZonedDateTime.class)
                .setCreateable(false)
                .setUpdateable(false)
                .setReturnedByDefault(true)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_LAST_MODIFIED_DATE)
                .setType(ZonedDateTime.class)
                .setCreateable(false)
                .setUpdateable(false)
                .setReturnedByDefault(true)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_DESCRIPTION)
                .setReturnedByDefault(true)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_PRECEDENCE)
                .setType(Integer.class)
                .setReturnedByDefault(true)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_ROLE_ARN)
                .setReturnedByDefault(true)
                .build());

        // Association
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_USERS)
                .setCreateable(true)
                .setUpdateable(true)
                .setReturnedByDefault(false)
                .setMultiValued(true)
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
     */
    public Uid createGroup(Set<Attribute> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            throw new InvalidAttributeValueException("attributes not provided or empty");
        }

        CreateGroupRequest.Builder request = CreateGroupRequest.builder()
                .userPoolId(configuration.getUserPoolID());

        List<Object> users = null;

        for (Attribute a : attributes) {
            switch (a.getName()) {
                case "__UID__":
                case "__NAME__":
                    request.groupName(AttributeUtil.getAsStringValue(a));
                    break;
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
        }

        CreateGroupResponse result = client.createGroup(request.build());

        checkCognitoResult(result, "CreateGroup");

        GroupType group = result.group();
        Uid newUid = new Uid(group.groupName(), group.groupName());

        // We need to call another API to add/remove user for this group.
        // It means that we can't execute this update as a single transaction.
        // Therefore, Cognito data may be inconsistent if below calling is failed.
        // Although this connector doesn't handle this situation, IDM can retry the update to resolve this inconsistency.
        userGroupHandler.updateUsersToGroup(newUid, users);

        return newUid;
    }

    /**
     * The spec for UpdateGroup:
     * https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_UpdateGroup.html
     *
     * @param objectClass
     * @param uid
     * @param replaceAttributes
     * @param operationOptions
     * @return
     */
    public Uid updateGroup(ObjectClass objectClass, Uid uid, Set<Attribute> replaceAttributes, OperationOptions operationOptions) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        UpdateGroupRequest.Builder request = UpdateGroupRequest.builder()
                .userPoolId(configuration.getUserPoolID())
                .groupName(uid.getUidValue());

        List<Object> users = null;

        for (Attribute a : replaceAttributes) {
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
        }

        try {
            UpdateGroupResponse result = client.updateGroup(request.build());

            checkCognitoResult(result, "UpdateGroup");
        } catch (ResourceNotFoundException e) {
            LOGGER.warn("Not found group when updating. uid: {0}", uid);
            throw new UnknownUidException(uid, objectClass);
        }

        // We need to call another API to add/remove user for this group.
        // It means that we can't execute this update as a single transaction.
        // Therefore, Cognito data may be inconsistent if below calling is failed.
        // Although this connector doesn't handle this situation, IDM can retry the update to resolve this inconsistency.
        userGroupHandler.updateUsersToGroup(uid, users);

        return uid;
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
            return toConnectorObject(g);
        }

        ConnectorObjectBuilder builder = new ConnectorObjectBuilder()
                .setObjectClass(ObjectClass.GROUP)
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

    private ConnectorObject toConnectorObject(GroupType g) {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder()
                .setObjectClass(ObjectClass.GROUP)
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

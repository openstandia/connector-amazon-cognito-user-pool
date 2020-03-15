package jp.openstandia.connector.amazonaws;

import com.amazonaws.services.cognitoidp.AWSCognitoIdentityProvider;
import com.amazonaws.services.cognitoidp.model.*;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;

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
    private final AWSCognitoIdentityProvider client;
    private final CognitoUserGroupHandler userGroupHandler;

    public CognitoGroupHandler(CognitoUserPoolConfiguration configuration, AWSCognitoIdentityProvider client) {
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

        CreateGroupRequest request = new CreateGroupRequest()
                .withUserPoolId(configuration.getUserPoolID());

        List<Object> users = null;
        ;

        for (Attribute a : attributes) {
            switch (a.getName()) {
                case "__UID__":
                case "__NAME__":
                    request.setGroupName(AttributeUtil.getAsStringValue(a));
                    break;
                case ATTR_DESCRIPTION:
                    request.setDescription(AttributeUtil.getAsStringValue(a));
                    break;
                case ATTR_PRECEDENCE:
                    request.setPrecedence(AttributeUtil.getIntegerValue(a));
                    break;
                case ATTR_ROLE_ARN:
                    request.setRoleArn(AttributeUtil.getAsStringValue(a));
                    break;
                case ATTR_USERS:
                    users = a.getValue();
                    break;
            }
        }

        CreateGroupResult result = client.createGroup(request);

        checkCognitoResult(result, "CreateGroup");

        GroupType group = result.getGroup();
        Uid newUid = new Uid(group.getGroupName(), group.getGroupName());

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

        UpdateGroupRequest request = new UpdateGroupRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withGroupName(uid.getUidValue());

        List<Object> users = null;

        for (Attribute a : replaceAttributes) {
            switch (a.getName()) {
                case ATTR_DESCRIPTION:
                    request.setDescription(AttributeUtil.getAsStringValue(a));
                    break;
                case ATTR_PRECEDENCE:
                    request.setPrecedence(AttributeUtil.getIntegerValue(a));
                    break;
                case ATTR_ROLE_ARN:
                    request.setRoleArn(AttributeUtil.getAsStringValue(a));
                    break;
                case ATTR_USERS:
                    users = a.getValue();
                    break;
            }
        }

        try {
            UpdateGroupResult result = client.updateGroup(request);

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

            DeleteGroupResult result = client.deleteGroup(new DeleteGroupRequest()
                    .withUserPoolId(configuration.getUserPoolID())
                    .withGroupName(uid.getUidValue()));

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
        ListGroupsRequest request = new ListGroupsRequest();
        request.setUserPoolId(configuration.getUserPoolID());

        String nextToken = null;

        do {
            request.setNextToken(nextToken);

            ListGroupsResult result = client.listGroups(request);

            result.getGroups().stream()
                    .forEach(u -> {
                        resultsHandler.handle(toConnectorObject(u, options));
                    });

            nextToken = result.getNextToken();

        } while (nextToken != null);
    }

    private void getGroupByName(String groupName,
                                ResultsHandler resultsHandler, OperationOptions options) {
        GetGroupResult result = client.getGroup(new GetGroupRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withGroupName(groupName));

        checkCognitoResult(result, "GetGroup");

        resultsHandler.handle(toConnectorObject(result.getGroup(), options));
    }

    private ConnectorObject toConnectorObject(GroupType g, OperationOptions options) {
        String[] attributesToGet = options.getAttributesToGet();
        if (attributesToGet == null) {
            return toConnectorObject(g);
        }

        ConnectorObjectBuilder builder = new ConnectorObjectBuilder()
                .setObjectClass(ObjectClass.GROUP)
                .setUid(g.getGroupName())
                .setName(g.getGroupName());

        for (String getAttr : attributesToGet) {
            switch (getAttr) {
                case ATTR_DESCRIPTION:
                    builder.addAttribute(ATTR_DESCRIPTION, g.getDescription());
                case ATTR_PRECEDENCE:
                    builder.addAttribute(ATTR_PRECEDENCE, g.getPrecedence());
                case ATTR_ROLE_ARN:
                    builder.addAttribute(ATTR_ROLE_ARN, g.getRoleArn());
                case ATTR_CREATION_DATE:
                    builder.addAttribute(ATTR_CREATION_DATE, toZoneDateTime(g.getCreationDate()));
                case ATTR_LAST_MODIFIED_DATE:
                    builder.addAttribute(ATTR_LAST_MODIFIED_DATE, toZoneDateTime(g.getLastModifiedDate()));
                case ATTR_USERS:
                    builder.addAttribute(ATTR_USERS, userGroupHandler.getUsersInGroup(g.getGroupName()));
            }
        }

        return builder.build();
    }

    private ConnectorObject toConnectorObject(GroupType g) {
        ConnectorObjectBuilder builder = new ConnectorObjectBuilder()
                .setObjectClass(ObjectClass.GROUP)
                .setUid(new Uid(g.getGroupName(), g.getGroupName()))
                .setName(g.getGroupName())
                .addAttribute(ATTR_DESCRIPTION, g.getDescription())
                .addAttribute(ATTR_PRECEDENCE, g.getPrecedence())
                .addAttribute(ATTR_ROLE_ARN, g.getRoleArn())
                .addAttribute(ATTR_CREATION_DATE, toZoneDateTime(g.getCreationDate()))
                .addAttribute(ATTR_LAST_MODIFIED_DATE, toZoneDateTime(g.getLastModifiedDate()));

        return builder.build();
    }
}

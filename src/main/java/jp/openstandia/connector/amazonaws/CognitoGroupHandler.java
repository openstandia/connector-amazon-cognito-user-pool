package jp.openstandia.connector.amazonaws;

import com.amazonaws.services.cognitoidp.AWSCognitoIdentityProvider;
import com.amazonaws.services.cognitoidp.model.*;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;

import java.util.Map;
import java.util.Set;

public class CognitoGroupHandler {

    private static final Log LOGGER = Log.getLog(CognitoGroupHandler.class);

    private static final String ATTR_GROUP_CREATION_DATE = "CreationDate";
    private static final String ATTR_GROUP_DESCRIPTION = "Description";
    private static final String ATTR_GROUP_GROUP_NAME = "GroupName";
    private static final String ATTR_GROUP_LAST_MODIFIED_DATE = "LastModifiedDate";
    private static final String ATTR_GROUP_PRECEDENCE = "Precedence";
    private static final String ATTR_GROUP_ROLE_ARN = "RoleArn";

    private final CognitoUserPoolConfiguration configuration;
    private final AWSCognitoIdentityProvider client;

    public CognitoGroupHandler(CognitoUserPoolConfiguration configuration, AWSCognitoIdentityProvider client) {
        this.configuration = configuration;
        this.client = client;
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
                .setNativeName(ATTR_GROUP_GROUP_NAME)
                .build());
        // __NAME__
        builder.addAttributeInfo(AttributeInfoBuilder.define(Name.NAME)
                .setRequired(true)
                .setCreateable(true)
                .setUpdateable(false)
                .setReturnedByDefault(true)
                .setNativeName(ATTR_GROUP_GROUP_NAME)
                .build());

        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_GROUP_CREATION_DATE)
                .setType(Integer.class)
                .setCreateable(false)
                .setUpdateable(false)
                .setReturnedByDefault(true)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_GROUP_LAST_MODIFIED_DATE)
                .setType(Integer.class)
                .setCreateable(false)
                .setUpdateable(false)
                .setReturnedByDefault(true)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_GROUP_DESCRIPTION)
                .setReturnedByDefault(true)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_GROUP_PRECEDENCE)
                .setType(Integer.class)
                .setReturnedByDefault(true)
                .build());
        builder.addAttributeInfo(AttributeInfoBuilder.define(ATTR_GROUP_ROLE_ARN)
                .setReturnedByDefault(true)
                .build());

        ObjectClassInfo groupSchemaInfo = builder.build();

        LOGGER.info("The constructed Group core schema: {0}", groupSchemaInfo);

        return groupSchemaInfo;
    }

    public Uid createGroup(Set<Attribute> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            throw new InvalidAttributeValueException("attributes not provided or empty");
        }

        CreateGroupRequest request = new CreateGroupRequest()
                .withUserPoolId(configuration.getUserPoolID());

        attributes.stream().forEach(a -> {
            switch (a.getName()) {
                case "__UID__":
                    request.setGroupName(AttributeUtil.getAsStringValue(a));
                    break;
                case ATTR_GROUP_DESCRIPTION:
                    request.setDescription(AttributeUtil.getAsStringValue(a));
                    break;
                case ATTR_GROUP_PRECEDENCE:
                    request.setPrecedence(AttributeUtil.getIntegerValue(a));
                    break;
                case ATTR_GROUP_ROLE_ARN:
                    request.setRoleArn(AttributeUtil.getAsStringValue(a));
                    break;
            }
        });

        CreateGroupResult response = client.createGroup(request);

        int status = response.getSdkHttpMetadata().getHttpStatusCode();
        if (status != 200) {
            throw new ConnectorException(String.format("Failed to createGroup. CreateGroup returned %d error.", status));
        }

        GroupType group = response.getGroup();
        return new Uid(group.getGroupName());
    }

    /**
     * The spec for UpdateGroup:
     * https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_UpdateGroup.html
     *
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

        replaceAttributes.stream().forEach(a -> {
            switch (a.getName()) {
                case ATTR_GROUP_DESCRIPTION:
                    request.setDescription(AttributeUtil.getAsStringValue(a));
                    break;
                case ATTR_GROUP_PRECEDENCE:
                    request.setPrecedence(AttributeUtil.getIntegerValue(a));
                    break;
                case ATTR_GROUP_ROLE_ARN:
                    request.setRoleArn(AttributeUtil.getAsStringValue(a));
                    break;
            }
        });

        UpdateGroupResult result = client.updateGroup(request);

        int status = result.getSdkHttpMetadata().getHttpStatusCode();
        if (status != 200) {
            throw new ConnectorException(String.format("Failed to updateGroup. UpdateGroup returned %d error.", status));
        }

        return uid;
    }

    public void deleteGroup(ObjectClass objectClass, Uid uid, OperationOptions options) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        DeleteGroupResult result = client.deleteGroup(new DeleteGroupRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withGroupName(uid.getUidValue()));

        int status = result.getSdkHttpMetadata().getHttpStatusCode();
        if (status != 200) {
            throw new ConnectorException(String.format("Failed to deleteGroup. DeleteGroup returned %d error.", status));
        }
    }

    public void getGroupByName(String groupName, ResultsHandler resultsHandler, OperationOptions operationOptions) {
        GetGroupResult result = client.getGroup(new GetGroupRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withGroupName(groupName));

        int status = result.getSdkHttpMetadata().getHttpStatusCode();
        if (status != 200) {
            throw new ConnectorException(String.format("Failed to get group by groupName. GetGroup returned %d status. groupName: %s", status, groupName));
        }

        resultsHandler.handle(toConnectorObject(result.getGroup()));
    }


    /**
     * The spec for ListGroups:
     * https://docs.aws.amazon.com/cognito-user-identity-pools/latest/APIReference/API_ListGroups.html
     *
     * @param schema
     * @param filter
     * @param resultsHandler
     * @param operationOptions
     */
    public void getGroups(Map<String, AttributeInfo> schema, CognitoUserPoolFilter filter, ResultsHandler resultsHandler, OperationOptions operationOptions) {
        // Cannot filter using Cognito API unfortunately...
        // So we only return all groups here.
        ListGroupsRequest request = new ListGroupsRequest();
        request.setUserPoolId(configuration.getUserPoolID());

        String nextToken = null;

        do {
            request.setNextToken(nextToken);

            ListGroupsResult result = client.listGroups(request);

            result.getGroups().stream()
                    .forEach(u -> {
                        resultsHandler.handle(toConnectorObject(u));
                    });

            nextToken = result.getNextToken();

        } while (nextToken != null);
    }

    private ConnectorObject toConnectorObject(GroupType g) { ConnectorObjectBuilder builder = new ConnectorObjectBuilder()
            .setUid(g.getGroupName())
            .setName(g.getGroupName())
            .addAttribute(ATTR_GROUP_DESCRIPTION, g.getDescription())
            .addAttribute(ATTR_GROUP_PRECEDENCE, g.getPrecedence())
            .addAttribute(ATTR_GROUP_ROLE_ARN, g.getRoleArn())
            .addAttribute(ATTR_GROUP_CREATION_DATE, g.getCreationDate())
            .addAttribute(ATTR_GROUP_LAST_MODIFIED_DATE, g.getLastModifiedDate());

        return builder.build();
    }
}

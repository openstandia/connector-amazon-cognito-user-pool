package jp.openstandia.connector.amazonaws;

import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.framework.api.APIConfiguration;
import org.identityconnectors.framework.api.ConnectorFacade;
import org.identityconnectors.framework.api.ConnectorFacadeFactory;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.test.common.TestHelpers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;
import software.amazon.awssdk.services.cognitoidentityprovider.paginators.ListGroupsIterable;
import software.amazon.awssdk.services.cognitoidentityprovider.paginators.ListUsersInGroupIterable;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static jp.openstandia.connector.amazonaws.MockClient.buildSuccess;
import static org.junit.jupiter.api.Assertions.*;

class GroupTest {

    ConnectorFacade connector;
    MockClient mockClient;

    private CognitoUserPoolConfiguration newConfiguration() {
        CognitoUserPoolConfiguration conf = new CognitoUserPoolConfiguration();
        conf.setUserPoolID("testPool");
        return conf;
    }

    private ConnectorFacade newFacade() {
        ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        APIConfiguration impl = TestHelpers.createTestConfiguration(LocalCognitoUserPoolConnector.class, newConfiguration());
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
    void createGroup() {
        // Given
        String groupName = "g1";
        String description = "desc";
        Integer precedence = 1;
        String roleArn = "role";

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(groupName));
        attrs.add(AttributeBuilder.build("Description", CollectionUtil.newSet(description)));
        attrs.add(AttributeBuilder.build("Precedence", CollectionUtil.newSet(precedence)));
        attrs.add(AttributeBuilder.build("RoleArn", CollectionUtil.newSet(roleArn)));

        mockClient.createGroup(request -> {
            CreateGroupResponse.Builder builder = CreateGroupResponse.builder()
                    .group(newGroupType(groupName, description, precedence, roleArn));
            return buildSuccess(builder, CreateGroupResponse.class);
        });

        // When
        Uid uid = connector.create(CognitoUserPoolGroupHandler.GROUP_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());

        // Then
        assertEquals(groupName, uid.getUidValue());
        assertNull(uid.getNameHint(), "Group shouldn't include Name object in the Uid" );
    }

    @Test
    void updateGroup() {
        // Given
        String groupName = "g1";
        String newDescription = "newDesc";
        Integer precedence = 1;
        String roleArn = "role";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("Description", CollectionUtil.newSet(newDescription)));

        AtomicReference<String> requestedDesc = new AtomicReference<>();
        mockClient.updateGroup(request -> {
            requestedDesc.set(request.description());

            UpdateGroupResponse.Builder builder = UpdateGroupResponse.builder();
            return buildSuccess(builder, UpdateGroupResponse.class);
        });

        // When
        Set<AttributeDelta> updated = connector.updateDelta(CognitoUserPoolGroupHandler.GROUP_OBJECT_CLASS,
                new Uid(groupName, new Name(groupName)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertEquals(newDescription, requestedDesc.get());
    }

    @Test
    void deleteGroup() {
        // Given
        String groupName = "g1";

        mockClient.listUsersInGroup(request -> {
            ListUsersInGroupResponse.Builder builder = ListUsersInGroupResponse.builder()
                    .users(newUserType("user", "sub", "user@example.com"));
            return buildSuccess(builder, ListUsersInGroupResponse.class);
        });

        mockClient.listUsersInGroupPaginator(request -> {
            ListUsersInGroupIterable response = new ListUsersInGroupIterable(mockClient, request);
            return response;
        });

        List<String> removeUser = new ArrayList<>();
        mockClient.adminRemoveUserFromGroup(request -> {
            removeUser.add(request.username());

            return buildSuccess(AdminRemoveUserFromGroupResponse.builder(), AdminRemoveUserFromGroupResponse.class);
        });

        AtomicReference<String> requestedGroupName = new AtomicReference<>();
        mockClient.deleteGroup(request -> {
            requestedGroupName.set(request.groupName());

            return buildSuccess(DeleteGroupResponse.builder(), DeleteGroupResponse.class);
        });

        // When
        connector.delete(CognitoUserPoolGroupHandler.GROUP_OBJECT_CLASS,
                new Uid(groupName, new Name(groupName)), new OperationOptionsBuilder().build());

        // Then
        assertEquals(groupName, requestedGroupName.get());
        assertEquals(1, removeUser.size());
        assertEquals("user", removeUser.get(0));
    }

    @Test
    void getGroup() {
        // Given
        String groupName = "g1";
        String description = "desc";
        Integer precedence = 1;
        String roleArn = "role";

        mockClient.getGroup(request -> {
            GetGroupResponse.Builder builer = GetGroupResponse.builder()
                    .group(newGroupType(groupName, description, precedence, roleArn));

            return buildSuccess(builer, GetGroupResponse.class);
        });

        // When
        ConnectorObject result = connector.getObject(CognitoUserPoolGroupHandler.GROUP_OBJECT_CLASS,
                new Uid(groupName, new Name(groupName)), new OperationOptionsBuilder().build());

        // Then
        assertEquals(groupName, result.getUid().getUidValue());
        assertEquals(groupName, result.getName().getNameValue());
        assertNotNull(result.getAttributeByName("Description"));
        assertEquals(description, result.getAttributeByName("Description").getValue().get(0));
        assertNotNull(result.getAttributeByName("Precedence"));
        assertEquals(precedence, result.getAttributeByName("Precedence").getValue().get(0));
        assertNotNull(result.getAttributeByName("RoleArn"));
        assertEquals(roleArn, result.getAttributeByName("RoleArn").getValue().get(0));
    }

    @Test
    void getAllUsers() {
        // Given
        mockClient.listGroupsPaginator(request -> {
            ListGroupsIterable response = new ListGroupsIterable(mockClient, request);
            return response;
        });

        mockClient.listGroups(request -> {
            ListGroupsResponse.Builder builer = ListGroupsResponse.builder()
                    .groups(
                            newGroupType("g1", "desc1", 1, "role1"),
                            newGroupType("g2", "desc2", 2, "role2")
                    );

            return buildSuccess(builer, ListGroupsResponse.class);
        });

        // When
        List<ConnectorObject> groups = new ArrayList<>();
        ResultsHandler handler = connectorObject -> {
            groups.add(connectorObject);
            return true;
        };
        connector.search(CognitoUserPoolGroupHandler.GROUP_OBJECT_CLASS,
                null, handler, new OperationOptionsBuilder().build());

        // Then
        assertEquals(2, groups.size());
        assertEquals("g1", groups.get(0).getUid().getUidValue());
        assertEquals("g1", groups.get(0).getName().getNameValue());
        assertEquals("desc1", groups.get(0).getAttributeByName("Description").getValue().get(0));
        assertEquals(1, groups.get(0).getAttributeByName("Precedence").getValue().get(0));
        assertEquals("role1", groups.get(0).getAttributeByName("RoleArn").getValue().get(0));
        assertEquals("g2", groups.get(1).getUid().getUidValue());
        assertEquals("g2", groups.get(1).getName().getNameValue());
        assertEquals("desc2", groups.get(1).getAttributeByName("Description").getValue().get(0));
        assertEquals(2, groups.get(1).getAttributeByName("Precedence").getValue().get(0));
        assertEquals("role2", groups.get(1).getAttributeByName("RoleArn").getValue().get(0));
    }

    private GroupType newGroupType(String groupName, String description, Integer precedence, String roleArn) {
        return GroupType.builder()
                .groupName(groupName)
                .description(description)
                .precedence(precedence)
                .roleArn(roleArn)
                .creationDate(Instant.now())
                .lastModifiedDate(Instant.now())
                .build();
    }

    private UserType newUserType(String username, String sub, String email) {
        return UserType.builder()
                .username(username)
                .enabled(true)
                .userStatus(UserStatusType.FORCE_CHANGE_PASSWORD)
                .userCreateDate(Instant.now())
                .userLastModifiedDate(Instant.now())
                .attributes(
                        AttributeType.builder()
                                .name("sub")
                                .value(sub)
                                .build(),
                        AttributeType.builder()
                                .name("email")
                                .value(email)
                                .build()
                )
                .build();
    }
}

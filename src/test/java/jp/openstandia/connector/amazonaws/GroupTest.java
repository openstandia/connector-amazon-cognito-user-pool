/*
 *  Copyright Nomura Research Institute, Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package jp.openstandia.connector.amazonaws;

import jp.openstandia.connector.amazonaws.testutil.AbstractTest;
import org.identityconnectors.common.CollectionUtil;
import org.identityconnectors.framework.common.objects.*;
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

import static jp.openstandia.connector.amazonaws.testutil.MockClient.buildSuccess;
import static org.junit.jupiter.api.Assertions.*;

class GroupTest extends AbstractTest {

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
        assertEquals(CognitoUserPoolGroupHandler.GROUP_OBJECT_CLASS, result.getObjectClass());
        assertEquals(groupName, result.getUid().getUidValue());
        assertEquals(groupName, result.getName().getNameValue());
        assertNotNull(result.getAttributeByName("CreationDate"));
        assertNotNull(result.getAttributeByName("LastModifiedDate"));
        assertNotNull(result.getAttributeByName("Description"));
        assertEquals(description, result.getAttributeByName("Description").getValue().get(0));
        assertNotNull(result.getAttributeByName("Precedence"));
        assertEquals(precedence, result.getAttributeByName("Precedence").getValue().get(0));
        assertNotNull(result.getAttributeByName("RoleArn"));
        assertEquals(roleArn, result.getAttributeByName("RoleArn").getValue().get(0));
    }

    @Test
    void getGroupWithAttributesToGet() {
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
        OperationOptions options = new OperationOptionsBuilder()
                .setAttributesToGet(
                        Uid.NAME,
                        Name.NAME,
                        "CreationDate"
                ).build();

        // When
        ConnectorObject result = connector.getObject(CognitoUserPoolGroupHandler.GROUP_OBJECT_CLASS,
                new Uid(groupName, new Name(groupName)), options);

        // Then
        assertEquals(CognitoUserPoolGroupHandler.GROUP_OBJECT_CLASS, result.getObjectClass());
        assertEquals(3, result.getAttributes().size());
        assertEquals(groupName, result.getUid().getUidValue());
        assertEquals(groupName, result.getName().getNameValue());
        assertNotNull(result.getAttributeByName("CreationDate"));
        assertNull(result.getAttributeByName("Description"));
        assertNull(result.getAttributeByName("Precedence"));
        assertNull(result.getAttributeByName("RoleArn"));
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
        assertEquals(CognitoUserPoolGroupHandler.GROUP_OBJECT_CLASS, groups.get(0).getObjectClass());
        assertEquals("g1", groups.get(0).getUid().getUidValue());
        assertEquals("g1", groups.get(0).getName().getNameValue());
        assertEquals("desc1", groups.get(0).getAttributeByName("Description").getValue().get(0));
        assertEquals(1, groups.get(0).getAttributeByName("Precedence").getValue().get(0));
        assertEquals("role1", groups.get(0).getAttributeByName("RoleArn").getValue().get(0));
        assertEquals(CognitoUserPoolGroupHandler.GROUP_OBJECT_CLASS, groups.get(1).getObjectClass());
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

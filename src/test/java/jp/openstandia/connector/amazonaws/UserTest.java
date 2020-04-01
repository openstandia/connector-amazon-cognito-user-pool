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
import software.amazon.awssdk.services.cognitoidentityprovider.paginators.ListUsersIterable;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static jp.openstandia.connector.amazonaws.testutil.MockClient.buildSuccess;
import static org.junit.jupiter.api.Assertions.*;

class UserTest extends AbstractTest {

    @Test
    void createUser() {
        // Given
        String username = "foo";
        String email = "foo@example.com";
        String sub = "00000000-0000-0000-0000-000000000001";

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(username));
        attrs.add(AttributeBuilder.build("email", CollectionUtil.newSet(email)));

        mockClient.adminCreateUser((request) -> {
            AdminCreateUserResponse.Builder builder = AdminCreateUserResponse.builder()
                    .user(newUserType(sub, username, email));
            return buildSuccess(builder, AdminCreateUserResponse.class);
        });

        // When
        Uid uid = connector.create(CognitoUserPoolUserHandler.USER_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());

        // Then
        assertEquals(sub, uid.getUidValue());
        assertEquals(username, uid.getNameHintValue());
    }

    @Test
    void createUserWithDisabled() {
        // Given
        String username = "foo";
        String email = "foo@example.com";
        String sub = "00000000-0000-0000-0000-000000000001";

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(username));
        attrs.add(AttributeBuilder.build("email", CollectionUtil.newSet(email)));
        attrs.add(AttributeBuilder.buildEnabled(false));

        mockClient.adminCreateUser(request -> {
            AdminCreateUserResponse.Builder builder = AdminCreateUserResponse.builder()
                    .user(newUserType(sub, username, email));
            return buildSuccess(builder, AdminCreateUserResponse.class);
        });
        mockClient.adminDisableUser(request -> {
            return buildSuccess(AdminDisableUserResponse.builder(), AdminDisableUserResponse.class);
        });

        // When
        Uid uid = connector.create(CognitoUserPoolUserHandler.USER_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());

        // Then
        assertEquals("00000000-0000-0000-0000-000000000001", uid.getUidValue());
        assertEquals("foo", uid.getNameHintValue());
    }

    @Test
    void createUserWithPassword() {
        // Given
        String username = "foo";
        String email = "foo@example.com";
        String sub = "00000000-0000-0000-0000-000000000001";

        Set<Attribute> attrs = new HashSet<>();
        attrs.add(new Name(username));
        attrs.add(AttributeBuilder.build("email", CollectionUtil.newSet(email)));
        attrs.add(AttributeBuilder.buildPassword("secret".toCharArray()));

        mockClient.adminCreateUser(request -> {
            AdminCreateUserResponse.Builder builder = AdminCreateUserResponse.builder()
                    .user(newUserType(sub, username, email));
            return buildSuccess(builder, AdminCreateUserResponse.class);
        });
        mockClient.adminSetUserPassword(request -> {
            return buildSuccess(AdminSetUserPasswordResponse.builder(), AdminSetUserPasswordResponse.class);
        });

        // When
        Uid uid = connector.create(CognitoUserPoolUserHandler.USER_OBJECT_CLASS, attrs, new OperationOptionsBuilder().build());

        // Then
        assertEquals("00000000-0000-0000-0000-000000000001", uid.getUidValue());
        assertEquals("foo", uid.getNameHintValue());
    }


    @Test
    void updateUser() {
        // Given
        String username = "foo";
        String email = "foo@example.com";
        String newEmail = "bar@example.com";
        String sub = "00000000-0000-0000-0000-000000000001";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("email", CollectionUtil.newSet(newEmail)));

        AtomicReference<Optional<String>> requestedNewEmail = new AtomicReference<>();
        mockClient.adminUpdateUserAttributes(request -> {
            requestedNewEmail.set(request.userAttributes().stream()
                    .filter(a -> a.name().equals("email"))
                    .map(a -> a.value())
                    .findFirst());

            AdminUpdateUserAttributesResponse.Builder builder = AdminUpdateUserAttributesResponse.builder();
            return buildSuccess(builder, AdminUpdateUserAttributesResponse.class);
        });

        // When
        Set<AttributeDelta> updated = connector.updateDelta(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                new Uid(sub, new Name(username)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNotNull(requestedNewEmail.get());
        assertEquals(newEmail, requestedNewEmail.get().get());
    }

    @Test
    void updateUserWithDisabled() {
        // Given
        String username = "foo";
        String email = "foo@example.com";
        String newEmail = "bar@example.com";
        String sub = "00000000-0000-0000-0000-000000000001";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("email", CollectionUtil.newSet(newEmail)));
        modifications.add(AttributeDeltaBuilder.buildEnabled(false));

        AtomicReference<Optional<String>> requestedNewEmail = new AtomicReference<>();
        mockClient.adminUpdateUserAttributes(request -> {
            requestedNewEmail.set(request.userAttributes().stream()
                    .filter(a -> a.name().equals("email"))
                    .map(a -> a.value())
                    .findFirst());

            AdminUpdateUserAttributesResponse.Builder builder = AdminUpdateUserAttributesResponse.builder();
            return buildSuccess(builder, AdminUpdateUserAttributesResponse.class);
        });
        mockClient.adminDisableUser(request -> {
            return buildSuccess(AdminDisableUserResponse.builder(), AdminDisableUserResponse.class);
        });

        // When
        Set<AttributeDelta> updated = connector.updateDelta(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                new Uid(sub, new Name(username)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNotNull(requestedNewEmail.get());
        assertEquals(newEmail, requestedNewEmail.get().get());
    }

    @Test
    void updateUserWithPassword() {
        // Given
        String username = "foo";
        String email = "foo@example.com";
        String newEmail = "bar@example.com";
        String sub = "00000000-0000-0000-0000-000000000001";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("email", CollectionUtil.newSet(newEmail)));
        modifications.add(AttributeDeltaBuilder.buildPassword("secret".toCharArray()));

        AtomicReference<Optional<String>> requestedNewEmail = new AtomicReference<>();
        mockClient.adminUpdateUserAttributes(request -> {
            requestedNewEmail.set(request.userAttributes().stream()
                    .filter(a -> a.name().equals("email"))
                    .map(a -> a.value())
                    .findFirst());

            AdminUpdateUserAttributesResponse.Builder builder = AdminUpdateUserAttributesResponse.builder();
            return buildSuccess(builder, AdminUpdateUserAttributesResponse.class);
        });
        AtomicReference<String> requestedNewPassword = new AtomicReference<>();
        AtomicReference<Boolean> requestedPasswordPermanent = new AtomicReference<>();
        mockClient.adminSetUserPassword(request -> {
            requestedNewPassword.set(request.password());
            requestedPasswordPermanent.set(request.permanent());

            return buildSuccess(AdminSetUserPasswordResponse.builder(), AdminSetUserPasswordResponse.class);
        });

        // When
        Set<AttributeDelta> updated = connector.updateDelta(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                new Uid(sub, new Name(username)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertNotNull(requestedNewEmail.get());
        assertEquals(newEmail, requestedNewEmail.get().get());
        assertEquals("secret", requestedNewPassword.get());
        assertNull(requestedPasswordPermanent.get());
    }

    @Test
    void updateUserPasswordOnly() {
        // Given
        String username = "foo";
        String sub = "00000000-0000-0000-0000-000000000001";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.buildPassword("secret".toCharArray()));
        modifications.add(AttributeDeltaBuilder.build("password_permanent", CollectionUtil.newSet(true)));

        AtomicReference<String> requestedNewPassword = new AtomicReference<>();
        AtomicReference<Boolean> requestedPasswordPermanent = new AtomicReference<>();
        mockClient.adminSetUserPassword(request -> {
            requestedNewPassword.set(request.password());
            requestedPasswordPermanent.set(request.permanent());

            return buildSuccess(AdminSetUserPasswordResponse.builder(), AdminSetUserPasswordResponse.class);
        });

        // When
        Set<AttributeDelta> updated = connector.updateDelta(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                new Uid(sub, new Name(username)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertEquals("secret", requestedNewPassword.get());
        assertTrue(requestedPasswordPermanent.get());
    }

    @Test
    void deleteuser() {
        // Given
        String username = "foo";
        String sub = "00000000-0000-0000-0000-000000000001";

        AtomicReference<String> requestedUsername = new AtomicReference<>();
        mockClient.adminDeleteUser(request -> {
            requestedUsername.set(request.username());

            return buildSuccess(AdminDeleteUserResponse.builder(), AdminDeleteUserResponse.class);
        });

        // When
        connector.delete(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                new Uid(sub, new Name(username)), new OperationOptionsBuilder().build());

        // Then
        assertEquals(username, requestedUsername.get());
    }

    @Test
    void updateUserWithAddGroup() {
        // Given
        String username = "foo";
        String sub = "00000000-0000-0000-0000-000000000001";
        String groupName = "g1";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("groups", CollectionUtil.newSet(groupName), null));

        AtomicReference<String> requestedUsername = new AtomicReference<>();
        AtomicReference<String> requestedGroupName = new AtomicReference<>();
        mockClient.adminAddUserToGroup(request -> {
            requestedUsername.set(request.username());
            requestedGroupName.set(request.groupName());

            return buildSuccess(AdminAddUserToGroupResponse.builder(), AdminAddUserToGroupResponse.class);
        });

        // When
        Set<AttributeDelta> updated = connector.updateDelta(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                new Uid(sub, new Name(username)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertEquals(username, requestedUsername.get());
        assertEquals(groupName, requestedGroupName.get());
    }

    @Test
    void updateUserWithRemoveGroup() {
        // Given
        String username = "foo";
        String sub = "00000000-0000-0000-0000-000000000001";
        String groupName = "g1";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("groups", null, CollectionUtil.newSet(groupName)));

        AtomicReference<String> requestedUsername = new AtomicReference<>();
        AtomicReference<String> requestedGroupName = new AtomicReference<>();
        mockClient.adminRemoveUserFromGroup(request -> {
            requestedUsername.set(request.username());
            requestedGroupName.set(request.groupName());

            return buildSuccess(AdminRemoveUserFromGroupResponse.builder(), AdminRemoveUserFromGroupResponse.class);
        });

        // When
        Set<AttributeDelta> updated = connector.updateDelta(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                new Uid(sub, new Name(username)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertEquals(username, requestedUsername.get());
        assertEquals(groupName, requestedGroupName.get());
    }

    @Test
    void updateUserWithMultipleAddAndRemoveGroup() {
        // Given
        String username = "foo";
        String sub = "00000000-0000-0000-0000-000000000001";
        String addGroup1 = "g1";
        String addGroup2 = "g2";
        String removeGroup1 = "g3";
        String removeGroup2 = "g4";

        Set<AttributeDelta> modifications = new HashSet<>();
        modifications.add(AttributeDeltaBuilder.build("groups",
                CollectionUtil.newSet(addGroup1, addGroup2),
                CollectionUtil.newSet(removeGroup1, removeGroup2)));

        List<String> addGroup = new ArrayList<>();
        mockClient.adminAddUserToGroup(request -> {
            addGroup.add(request.groupName());

            return buildSuccess(AdminAddUserToGroupResponse.builder(), AdminAddUserToGroupResponse.class);
        });

        List<String> removeGroup = new ArrayList<>();
        mockClient.adminRemoveUserFromGroup(request -> {
            removeGroup.add(request.groupName());

            return buildSuccess(AdminRemoveUserFromGroupResponse.builder(), AdminRemoveUserFromGroupResponse.class);
        });

        // When
        Set<AttributeDelta> updated = connector.updateDelta(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                new Uid(sub, new Name(username)), modifications, new OperationOptionsBuilder().build());

        // Then
        assertEquals(2, addGroup.size());
        assertEquals(2, removeGroup.size());
        assertEquals(addGroup1, addGroup.get(0));
        assertEquals(addGroup2, addGroup.get(1));
        assertEquals(removeGroup1, removeGroup.get(0));
        assertEquals(removeGroup2, removeGroup.get(1));
    }

    @Test
    void getUser() {
        // Given
        String username = "foo";
        String sub = "00000000-0000-0000-0000-000000000001";
        String email = "foo@example.com";

        mockClient.adminGetUser(request -> {
            AdminGetUserResponse.Builder builer = AdminGetUserResponse.builder()
                    .username(username)
                    .enabled(true)
                    .userCreateDate(Instant.now())
                    .userLastModifiedDate(Instant.now())
                    .userAttributes(
                            AttributeType.builder()
                                    .name("sub")
                                    .value(sub)
                                    .build(),
                            AttributeType.builder()
                                    .name("email")
                                    .value(email)
                                    .build()
                    );

            return buildSuccess(builer, AdminGetUserResponse.class);
        });

        // When
        ConnectorObject result = connector.getObject(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                new Uid(sub, new Name(username)), new OperationOptionsBuilder().build());

        // Then
        assertEquals(CognitoUserPoolUserHandler.USER_OBJECT_CLASS, result.getObjectClass());
        assertEquals(sub, result.getUid().getUidValue());
        assertEquals(username, result.getName().getNameValue());
        assertNotNull(result.getAttributeByName("email"));
        assertEquals(email, result.getAttributeByName("email").getValue().get(0));
    }

    @Test
    void getUserWithAttributesToGet() {
        // Given
        String username = "foo";
        String sub = "00000000-0000-0000-0000-000000000001";
        String email = "foo@example.com";

        mockClient.adminGetUser(request -> {
            AdminGetUserResponse.Builder builer = AdminGetUserResponse.builder()
                    .username(username)
                    .enabled(true)
                    .userCreateDate(Instant.now())
                    .userLastModifiedDate(Instant.now())
                    .userAttributes(
                            AttributeType.builder()
                                    .name("sub")
                                    .value(sub)
                                    .build(),
                            AttributeType.builder()
                                    .name("email")
                                    .value(email)
                                    .build()
                    );

            return buildSuccess(builer, AdminGetUserResponse.class);
        });
        OperationOptions options = new OperationOptionsBuilder()
                .setAttributesToGet(
                        Uid.NAME,
                        Name.NAME,
                        "UserCreateDate"
                ).build();

        // When
        ConnectorObject result = connector.getObject(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                new Uid(sub, new Name(username)), options);

        // Then
        assertEquals(3, result.getAttributes().size());
        assertEquals(sub, result.getUid().getUidValue());
        assertEquals(username, result.getName().getNameValue());
        assertNull(result.getAttributeByName("email"));
        assertNotNull(result.getAttributeByName("UserCreateDate"));
    }

    @Test
    void getAllUsers() {
        // Given
        mockClient.listUsersPaginator(request -> {
            ListUsersIterable response = new ListUsersIterable(mockClient, request);
            return response;
        });

        mockClient.listUsers(request -> {
            ListUsersResponse.Builder builer = ListUsersResponse.builder()
                    .users(
                            newUserType("sub1", "user1", "user1@example.com"),
                            newUserType("sub2", "user2", "user2@example.com")
                    );

            return buildSuccess(builer, ListUsersResponse.class);
        });

        // When
        List<ConnectorObject> users = new ArrayList<>();
        ResultsHandler handler = connectorObject -> {
            users.add(connectorObject);
            return true;
        };
        connector.search(CognitoUserPoolUserHandler.USER_OBJECT_CLASS,
                null, handler, new OperationOptionsBuilder().build());

        // Then
        assertEquals(2, users.size());
        assertEquals(CognitoUserPoolUserHandler.USER_OBJECT_CLASS, users.get(0).getObjectClass());
        assertEquals("sub1", users.get(0).getUid().getUidValue());
        assertEquals("user1", users.get(0).getName().getNameValue());
        assertEquals("user1@example.com", users.get(0).getAttributeByName("email").getValue().get(0));
        assertEquals(CognitoUserPoolUserHandler.USER_OBJECT_CLASS, users.get(1).getObjectClass());
        assertEquals("sub2", users.get(1).getUid().getUidValue());
        assertEquals("user2", users.get(1).getName().getNameValue());
        assertEquals("user2@example.com", users.get(1).getAttributeByName("email").getValue().get(0));
    }

    private UserType newUserType(String sub, String username, String email) {
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

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

import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static jp.openstandia.connector.amazonaws.MockClient.buildSuccess;
import static org.junit.jupiter.api.Assertions.*;

class UserTest {

    ConnectorFacade connector;
    MockClient mockClient;

    private CognitoUserPoolConfiguration newConfiguration() {
        CognitoUserPoolConfiguration conf = new CognitoUserPoolConfiguration();
        conf.setUserPoolID("testPool");
        return conf;
    }

    private ConnectorFacade newFacade() {
        ConnectorFacadeFactory factory = ConnectorFacadeFactory.getInstance();
        APIConfiguration impl = TestHelpers.createTestConfiguration(TestConnector.class, newConfiguration());
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
    void schema() {
        Schema schema = connector.schema();

        assertNotNull(schema);
        assertEquals(2, schema.getObjectClassInfo().size());

        Optional<ObjectClassInfo> user = schema.getObjectClassInfo().stream().filter(o -> o.is("User")).findFirst();
        Optional<ObjectClassInfo> group = schema.getObjectClassInfo().stream().filter(o -> o.is("Group")).findFirst();

        assertTrue(user.isPresent());
        assertTrue(group.isPresent());
    }

    @Test
    void test() {
        connector.test();
    }

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
        assertNotNull(requestedNewPassword.get());
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
        assertNotNull(requestedNewPassword.get());
        assertEquals("secret", requestedNewPassword.get());
        assertNotNull(requestedPasswordPermanent.get());
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
        assertNotNull(requestedUsername.get());
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

package jp.openstandia.connector.amazonaws;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.SdkResponse;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;

import java.util.function.Function;

public class MockClient implements CognitoIdentityProviderClient {

    private static final MockClient INSTANCE = new MockClient();

    public boolean closed = false;

    private Function<AdminCreateUserRequest, AdminCreateUserResponse> adminCreateUser;
    private Function<AdminEnableUserRequest, AdminEnableUserResponse> adminEnableUser;
    private Function<AdminDisableUserRequest, AdminDisableUserResponse> adminDisableUser;
    private Function<AdminSetUserPasswordRequest, AdminSetUserPasswordResponse> adminSetUserPassword;
    private Function<AdminUpdateUserAttributesRequest, AdminUpdateUserAttributesResponse> adminUpdateUserAttributes;
    private Function<AdminDeleteUserRequest, AdminDeleteUserResponse> adminDeleteUser;
    private Function<AdminAddUserToGroupRequest, AdminAddUserToGroupResponse> adminAddUserToGroup;
    private Function<AdminRemoveUserFromGroupRequest, AdminRemoveUserFromGroupResponse> adminRemoveUserFromGroup;

    private MockClient() {
    }

    public static MockClient instance() {
        return INSTANCE;
    }

    public static <T> T buildSuccess(CognitoIdentityProviderResponse.Builder builder, Class<T> clazz) {
        SdkResponse response = builder.sdkHttpResponse(SdkHttpResponse.builder().statusCode(200).build()).build();
        return (T)response;
    }

    public void init() {
        closed = false;
        adminCreateUser = null;
        adminEnableUser  = null;
        adminDisableUser = null;
        adminSetUserPassword = null;
        adminUpdateUserAttributes = null;
    }

    @Override
    public String serviceName() {
        return "mock";
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public DescribeUserPoolResponse describeUserPool(DescribeUserPoolRequest request) throws AwsServiceException, SdkClientException {
        DescribeUserPoolResponse.Builder builder = DescribeUserPoolResponse.builder()
                .userPool(UserPoolType.builder()
                        .schemaAttributes(
                                SchemaAttributeType.builder()
                                        .name("sub")
                                        .attributeDataType(AttributeDataType.STRING)
                                        .mutable(false)
                                        .required(false)
                                        .build(),
                                SchemaAttributeType.builder()
                                        .name("email")
                                        .attributeDataType(AttributeDataType.STRING)
                                        .mutable(true)
                                        .required(false)
                                        .build(),
                                SchemaAttributeType.builder()
                                        .name("custom:string")
                                        .attributeDataType(AttributeDataType.STRING)
                                        .mutable(true)
                                        .required(false)
                                        .build(),
                                SchemaAttributeType.builder()
                                        .name("custom:integer")
                                        .attributeDataType(AttributeDataType.NUMBER)
                                        .mutable(true)
                                        .required(false)
                                        .build(),
                                SchemaAttributeType.builder()
                                        .name("custom:datetime")
                                        .attributeDataType(AttributeDataType.DATE_TIME)
                                        .mutable(true)
                                        .required(false)
                                        .build(),
                                SchemaAttributeType.builder()
                                        .name("custom:boolean")
                                        .attributeDataType(AttributeDataType.BOOLEAN)
                                        .mutable(true)
                                        .required(false)
                                        .build()
                        )
                        .usernameConfiguration(UsernameConfigurationType.builder().caseSensitive(true).build())
                        .build());

        return buildSuccess(builder, DescribeUserPoolResponse.class);
    }

    public void adminCreateUser(Function<AdminCreateUserRequest, AdminCreateUserResponse> mock) {
        this.adminCreateUser = mock;
    }

    @Override
    public AdminCreateUserResponse adminCreateUser(AdminCreateUserRequest request) throws AwsServiceException, SdkClientException {
        return adminCreateUser.apply(request);
    }

    public void adminEnableUser(Function<AdminEnableUserRequest, AdminEnableUserResponse> mock) {
        this.adminEnableUser = mock;
    }

    @Override
    public AdminEnableUserResponse adminEnableUser(AdminEnableUserRequest request) throws AwsServiceException, SdkClientException {
        return adminEnableUser.apply(request);
    }

    public void adminDisableUser(Function<AdminDisableUserRequest, AdminDisableUserResponse> mock) {
        this.adminDisableUser = mock;
    }

    @Override
    public AdminDisableUserResponse adminDisableUser(AdminDisableUserRequest request) throws AwsServiceException, SdkClientException {
        return adminDisableUser.apply(request);
    }

    public void adminSetUserPassword(Function<AdminSetUserPasswordRequest, AdminSetUserPasswordResponse> mock) {
        this.adminSetUserPassword = mock;
    }

    @Override
    public AdminSetUserPasswordResponse adminSetUserPassword(AdminSetUserPasswordRequest request) throws AwsServiceException, SdkClientException {
        return adminSetUserPassword.apply(request);
    }

    public void adminUpdateUserAttributes(Function<AdminUpdateUserAttributesRequest, AdminUpdateUserAttributesResponse> mock) {
        this.adminUpdateUserAttributes = mock;
    }

    @Override
    public AdminDeleteUserResponse adminDeleteUser(AdminDeleteUserRequest request) throws AwsServiceException, SdkClientException {
        return adminDeleteUser.apply(request);
    }

    public void adminDeleteUser(Function<AdminDeleteUserRequest, AdminDeleteUserResponse> mock) {
        this.adminDeleteUser = mock;
    }

    @Override
    public AdminAddUserToGroupResponse adminAddUserToGroup(AdminAddUserToGroupRequest request) throws AwsServiceException, SdkClientException {
        return adminAddUserToGroup.apply(request);
    }

    public void adminAddUserToGroup(Function<AdminAddUserToGroupRequest, AdminAddUserToGroupResponse> mock) {
        this.adminAddUserToGroup = mock;
    }

    @Override
    public AdminRemoveUserFromGroupResponse adminRemoveUserFromGroup(AdminRemoveUserFromGroupRequest request) throws AwsServiceException, SdkClientException {
        return adminRemoveUserFromGroup.apply(request);
    }

    public void adminRemoveUserFromGroup(Function<AdminRemoveUserFromGroupRequest, AdminRemoveUserFromGroupResponse> mock) {
        this.adminRemoveUserFromGroup = mock;
    }

    @Override
    public AdminUpdateUserAttributesResponse adminUpdateUserAttributes(AdminUpdateUserAttributesRequest request) throws AwsServiceException, SdkClientException {
        return this.adminUpdateUserAttributes.apply(request);
    }
}

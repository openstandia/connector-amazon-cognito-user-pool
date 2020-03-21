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

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.Uid;
import software.amazon.awssdk.services.cognitoidentityprovider.CognitoIdentityProviderClient;
import software.amazon.awssdk.services.cognitoidentityprovider.model.*;
import software.amazon.awssdk.services.cognitoidentityprovider.paginators.AdminListGroupsForUserIterable;
import software.amazon.awssdk.services.cognitoidentityprovider.paginators.ListUsersInGroupIterable;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static jp.openstandia.connector.amazonaws.CognitoUserPoolUtils.checkCognitoResult;

public class CognitoUserPoolAssociationHandler {

    private static final Log LOGGER = Log.getLog(CognitoUserPoolUserHandler.class);

    private final CognitoUserPoolConfiguration configuration;
    private final CognitoIdentityProviderClient client;

    public CognitoUserPoolAssociationHandler(CognitoUserPoolConfiguration configuration, CognitoIdentityProviderClient client) {
        this.configuration = configuration;
        this.client = client;
    }

    public void updateGroupsToUser(Name name, List<Object> addGroups, List<Object> removeGroups) {
        if (!addGroups.isEmpty()) {
            addGroups.stream().forEach(g -> addUserToGroup(name.getNameValue(), g.toString()));
        }
        if (!removeGroups.isEmpty()) {
            removeGroups.stream().forEach(g -> removeUserFromGroup(name.getNameValue(), g.toString()));
        }
    }

    public void updateGroupsToUser(Name name, List<Object> addGroups) {
        if (addGroups == null) {
            return;
        }

        Set<String> addGroupsSet = addGroups.stream()
                .map(o -> o.toString())
                .collect(Collectors.toSet());

        getGroups(name.getNameValue(), g -> {
            if (!addGroupsSet.remove(g.groupName())) {
                removeUserFromGroup(name.getNameValue(), g.groupName());
            }
        });

        // Add groups to the user
        addGroupsSet.forEach(g -> addUserToGroup(name.getNameValue(), g));
    }

    public void updateUsersToGroup(Uid groupUid, List<Object> addUsers, List<Object> removeUsers) {
        if (addUsers != null) {
            addUsers.stream().forEach(u -> addUserToGroup(u.toString(), groupUid.getUidValue()));
        }
        if (removeUsers != null) {
            removeUsers.stream().forEach(u -> removeUserFromGroup(u.toString(), groupUid.getUidValue()));
        }
    }

    public void updateUsersToGroup(Uid groupUid, List<Object> addUsers) {
        if (addUsers == null) {
            return;
        }

        Set<String> addUsersSet = addUsers.stream()
                .map(o -> o.toString())
                .collect(Collectors.toSet());

        getUsers(groupUid.getUidValue(), u -> {
            if (!addUsersSet.remove(u.username())) {
                removeUserFromGroup(u.username(), groupUid.getUidValue());
            }
        });

        // Add users to the group
        addUsersSet.forEach(u -> addUserToGroup(u, groupUid.getUidValue()));
    }

    private void addUserToGroup(String username, String groupName) {
        AdminAddUserToGroupRequest.Builder request = AdminAddUserToGroupRequest.builder()
                .userPoolId(configuration.getUserPoolID())
                .username(username)
                .groupName(groupName);

        AdminAddUserToGroupResponse result = client.adminAddUserToGroup(request.build());

        checkCognitoResult(result, "AdminAddUserToGroup");
    }

    private void removeUserFromGroup(String username, String groupName) {
        AdminRemoveUserFromGroupRequest.Builder request = AdminRemoveUserFromGroupRequest.builder()
                .userPoolId(configuration.getUserPoolID())
                .username(username)
                .groupName(groupName);

        AdminRemoveUserFromGroupResponse result = client.adminRemoveUserFromGroup(request.build());

        checkCognitoResult(result, "AdminRemoveUserFromGroup");
    }

    public void removeAllUsers(String groupName) {
        getUsers(groupName, u -> removeUserFromGroup(u.username(), groupName));
    }

    public List<String> getUsersInGroup(String groupName) {
        List<String> users = new ArrayList<>();
        getUsers(groupName, u -> {
            users.add(u.username());
        });
        return users;
    }


    private interface UserHandler {
        void handle(UserType user);
    }

    void getUsers(String groupName, UserHandler handler) {
        ListUsersInGroupRequest.Builder request = ListUsersInGroupRequest.builder()
                .userPoolId(configuration.getUserPoolID())
                .groupName(groupName);

        ListUsersInGroupIterable result = client.listUsersInGroupPaginator(request.build());

        result.forEach(r -> r.users().stream().forEach(u -> handler.handle(u)));
    }

    public List<String> getGroupsForUser(String username) {
        List<String> groups = new ArrayList<>();
        getGroups(username, g -> {
            groups.add(g.groupName());
        });
        return groups;
    }

    private interface GroupHandler {
        void handle(GroupType group);
    }

    private void getGroups(String userName, GroupHandler handler) {
        AdminListGroupsForUserRequest.Builder request = AdminListGroupsForUserRequest.builder()
                .userPoolId(configuration.getUserPoolID())
                .username(userName);

        AdminListGroupsForUserIterable result = client.adminListGroupsForUserPaginator(request.build());

        result.forEach(r -> r.groups().stream().forEach(g -> handler.handle(g)));
    }
}

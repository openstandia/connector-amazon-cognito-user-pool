package jp.openstandia.connector.amazonaws;

import com.amazonaws.services.cognitoidp.AWSCognitoIdentityProvider;
import com.amazonaws.services.cognitoidp.model.*;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.Uid;

import java.util.*;
import java.util.stream.Collectors;

import static jp.openstandia.connector.amazonaws.CognitoUtils.checkCognitoResult;

public class CognitoUserGroupHandler {

    private static final Log LOGGER = Log.getLog(CognitoUserHandler.class);

    private final CognitoUserPoolConfiguration configuration;
    private final AWSCognitoIdentityProvider client;

    public CognitoUserGroupHandler(CognitoUserPoolConfiguration configuration, AWSCognitoIdentityProvider client) {
        this.configuration = configuration;
        this.client = client;
    }

    public void updateGroupsToUser(Name name, List<Object> addGroups) {
        if (addGroups == null) {
            return;
        }

        Set<String> addGroupsSet = addGroups.stream()
                .map(o -> o.toString())
                .collect(Collectors.toSet());

        getGroups(name.getNameValue(), g -> {
            if(!addGroupsSet.remove(g.getGroupName())) {
                removeUserFromGroup(name.getNameValue(), g.getGroupName());
            }
        });

        // Add groups to the user
        addGroupsSet.forEach(g -> addUserToGroup(name.getNameValue(), g));
    }

    public void updateUsersToGroup(Uid groupUid, List<Object> addUsers) {
        if (addUsers == null) {
            return;
        }

        Set<String> addUsersSet = addUsers.stream()
                .map(o -> o.toString())
                .collect(Collectors.toSet());

        getUsers(groupUid.getUidValue(), u -> {
            if(!addUsersSet.remove(u.getUsername())) {
                removeUserFromGroup(u.getUsername(), groupUid.getUidValue());
            }
        });

        // Add users to the group
        addUsersSet.forEach(u -> addUserToGroup(u, groupUid.getUidValue()));
    }

    private void addUserToGroup(String username, String groupName) {
        AdminAddUserToGroupRequest request = new AdminAddUserToGroupRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withUsername(username)
                .withGroupName(groupName);

        AdminAddUserToGroupResult result = client.adminAddUserToGroup(request);

        checkCognitoResult(result, "AdminAddUserToGroup");
    }

    private void removeUserFromGroup(String username, String groupName) {
        AdminRemoveUserFromGroupRequest request = new AdminRemoveUserFromGroupRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withUsername(username)
                .withGroupName(groupName);

        AdminRemoveUserFromGroupResult result = client.adminRemoveUserFromGroup(request);

        checkCognitoResult(result, "AdminRemoveUserFromGroup");
    }

    public void removeAllUsers(String groupName) {
        getUsers(groupName, u -> removeUserFromGroup(u.getUsername(), groupName));
    }

    public List<String> getUsersInGroup(String groupName) {
        List<String> users = new ArrayList<>();
        getUsers(groupName, u -> {
           users.add(u.getUsername());
        });
        return users;
    }

    private interface UserHandler {
        void handle(UserType user);
    }

     void getUsers(String groupName, UserHandler handler) {
        ListUsersInGroupRequest request = new ListUsersInGroupRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withGroupName(groupName);

        String nextToken = null;

        do {
            request.setNextToken(nextToken);

            ListUsersInGroupResult result = client.listUsersInGroup(request);

            result.getUsers().stream()
                    .forEach(u -> handler.handle(u));

            nextToken = result.getNextToken();

        } while (nextToken != null);
    }

    public List<String> getGroupsForUser(String username) {
        List<String> groups = new ArrayList<>();
        getGroups(username, g -> {
            groups.add(g.getGroupName());
        });
        return groups;
    }

    private interface GroupHandler {
        void handle(GroupType group);
    }

    private void getGroups(String userName, GroupHandler handler) {
        AdminListGroupsForUserRequest request = new AdminListGroupsForUserRequest()
                .withUserPoolId(configuration.getUserPoolID())
                .withUsername(userName);

        String nextToken = null;

        do {
            request.setNextToken(nextToken);

            AdminListGroupsForUserResult result = client.adminListGroupsForUser(request);

            result.getGroups().stream()
                    .forEach(u -> handler.handle(u));

            nextToken = result.getNextToken();

        } while (nextToken != null);
    }
}

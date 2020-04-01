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
import software.amazon.awssdk.services.cognitoidentityprovider.model.UpdateGroupResponse;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static jp.openstandia.connector.amazonaws.testutil.MockClient.buildSuccess;
import static org.junit.jupiter.api.Assertions.assertEquals;

class GroupUpdateTest extends AbstractTest {

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
}

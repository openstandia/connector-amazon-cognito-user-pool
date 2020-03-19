package jp.openstandia.connector.amazonaws;

import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.http.SdkHttpResponse;
import software.amazon.awssdk.services.cognitoidentityprovider.model.AttributeType;
import software.amazon.awssdk.services.cognitoidentityprovider.model.ListUsersResponse;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CognitoUserPoolUtilsTest {

    @Test
    void toConnectorAttribute() {
        Attribute string = CognitoUserPoolUtils.toConnectorAttribute(
                AttributeInfoBuilder.define("string").setType(String.class).build(),
                AttributeType.builder().name("string").value("test").build());

        assertEquals("string", string.getName());
        assertEquals(1, string.getValue().size());
        assertEquals("test", string.getValue().get(0));

        Attribute integer = CognitoUserPoolUtils.toConnectorAttribute(
                AttributeInfoBuilder.define("int").setType(Integer.class).build(),
                AttributeType.builder().name("int").value("1").build());

        assertEquals("int", integer.getName());
        assertEquals(1, integer.getValue().size());
        assertEquals(1, integer.getValue().get(0));

        Attribute date = CognitoUserPoolUtils.toConnectorAttribute(
                AttributeInfoBuilder.define("date").setType(ZonedDateTime.class).build(),
                AttributeType.builder().name("date").value("2007-12-03").build());

        assertEquals("date", date.getName());
        assertEquals(1, date.getValue().size());
        assertEquals(LocalDateTime.parse("2007-12-03T00:00:00").atZone(ZoneId.systemDefault()),
                date.getValue().get(0));
    }

    @Test
    void toCognitoAttribute() {
        Map<String, AttributeInfo> schema = new HashMap<>();
        schema.put("string", AttributeInfoBuilder.define("string").setType(String.class).build());
        schema.put("int", AttributeInfoBuilder.define("int").setType(Integer.class).build());
        schema.put("date", AttributeInfoBuilder.define("date").setType(ZonedDateTime.class).build());
        schema.put("bool", AttributeInfoBuilder.define("bool").setType(Boolean.class).build());

        assertEquals("test",
                CognitoUserPoolUtils.toCognitoAttribute(schema,
                        AttributeBuilder.build("string", "test"))
                        .value()
        );
        assertEquals("1",
                CognitoUserPoolUtils.toCognitoAttribute(schema,
                        AttributeBuilder.build("int", 1))
                        .value()
        );
        assertEquals("2007-12-03",
                CognitoUserPoolUtils.toCognitoAttribute(schema,
                        AttributeBuilder.build("date",
                                LocalDateTime.parse("2007-12-03T10:15:30").atZone(ZoneId.systemDefault())))
                        .value()
        );
        assertEquals("true",
                CognitoUserPoolUtils.toCognitoAttribute(schema,
                        AttributeBuilder.build("bool", Boolean.TRUE))
                        .value()
        );

        // No schema case
        assertThrows(InvalidAttributeValueException.class,
                () -> CognitoUserPoolUtils.toCognitoAttribute(schema,
                        AttributeBuilder.build("foo", "test"))
                        .value()
        );
    }

    @Test
    void toCognitoAttributeForDelete() {
        AttributeType attributeType = CognitoUserPoolUtils.toCognitoAttributeForDelete(
                AttributeBuilder.build("foo", "test"));
        assertEquals("foo", attributeType.name());
        assertEquals("", attributeType.value());
    }

    @Test
    void checkCognitoResult() {
        SdkHttpResponse sdkHttpResponse = SdkHttpResponse.builder()
                .statusCode(400)
                .build();
        ListUsersResponse.Builder builder = ListUsersResponse.builder();
        builder.sdkHttpResponse(sdkHttpResponse);
        ListUsersResponse response = builder.build();

        assertThrows(ConnectorException.class,
                () -> CognitoUserPoolUtils.checkCognitoResult(response, "ListUsers"));
    }

    @Test
    void shouldReturnPartialAttributeValues() {
        OperationOptions noOptions = new OperationOptionsBuilder().build();
        assertFalse(CognitoUserPoolUtils.shouldReturnPartialAttributeValues(noOptions));

        OperationOptions falseOption = new OperationOptionsBuilder().setAllowPartialAttributeValues(false).build();
        assertFalse(CognitoUserPoolUtils.shouldReturnPartialAttributeValues(falseOption));

        OperationOptions trueOption = new OperationOptionsBuilder().setAllowPartialAttributeValues(true).build();
        assertTrue(CognitoUserPoolUtils.shouldReturnPartialAttributeValues(trueOption));
    }
}
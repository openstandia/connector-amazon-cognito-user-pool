package jp.openstandia.connector.amazonaws;

import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;
import software.amazon.awssdk.services.cognitoidentityprovider.model.AttributeType;
import software.amazon.awssdk.services.cognitoidentityprovider.model.CognitoIdentityProviderResponse;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/**
 * Provides utility methods
 *
 * @author Hiroyuki Wada
 */
public class CognitoUserPoolUtils {

    public static ZonedDateTime toZoneDateTime(Instant instant) {
        ZoneId zone = ZoneId.systemDefault();
        return ZonedDateTime.ofInstant(instant, zone);
    }

    public static ZonedDateTime toZoneDateTime(String yyyymmdd) {
        LocalDate date = LocalDate.parse(yyyymmdd);
        return date.atStartOfDay(ZoneId.systemDefault());
    }

    /**
     * Transform a Cognito attribute object to a Connector attribute object.
     *
     * @param attributeInfo
     * @param a
     * @return
     */
    public static Attribute toConnectorAttribute(AttributeInfo attributeInfo, AttributeType a) {
        // Cognito API returns the attribute as string even if it's other types.
        // We need to check the type from the schema and convert it.
        // Also, we must escape the name for custom attributes (The name of custom attribute starts with "custom:").
        if (attributeInfo.getType() == Integer.class) {
            return AttributeBuilder.build(escapeName(a.name()), Integer.parseInt(a.value()));
        }
        if (attributeInfo.getType() == ZonedDateTime.class) {
            // The format is YYYY-MM-DD
            return AttributeBuilder.build(escapeName(a.name()), toZoneDateTime(a.value()));
        }
        if (attributeInfo.getType() == Boolean.class) {
            return AttributeBuilder.build(escapeName(a.name()), Boolean.parseBoolean(a.value()));
        }

        // String
        return AttributeBuilder.build(escapeName(a.name()), a.value());
    }

    /**
     * Transform a Connector attribute object to a Cognito attribute object.
     *
     * @param schema
     * @param attr
     * @return
     */
    public static AttributeType toCognitoAttribute(Map<String, AttributeInfo> schema, Attribute attr) {
        return AttributeType.builder()
                .name(unescapeName(attr.getName()))
                .value(toCognitoValue(schema, attr))
                .build();
    }

    private static String toCognitoValue(Map<String, AttributeInfo> schema, Attribute attr) {
        // The key of the schema is escaped key
        AttributeInfo attributeInfo = schema.get(attr.getName());
        if (attributeInfo == null) {
            throw new InvalidAttributeValueException("");
        }

        if (attributeInfo.getType() == Integer.class) {
            return AttributeUtil.getAsStringValue(attr);
        }
        if (attributeInfo.getType() == ZonedDateTime.class) {
            // The format must be YYYY-MM-DD in cognito
            ZonedDateTime date = (ZonedDateTime) AttributeUtil.getSingleValue(attr);
            return date.format(DateTimeFormatter.ISO_LOCAL_DATE);
        }
        if (attributeInfo.getType() == Boolean.class) {
            return AttributeUtil.getAsStringValue(attr);
        }

        return AttributeUtil.getAsStringValue(attr);
    }

    /**
     * Transform a Connector attribute object to a Cognito attribute object for deleting the value.
     *
     * @param attr
     * @return
     */
    public static AttributeType toCognitoAttributeForDelete(Attribute attr) {
        // Cognito deletes the attribute when updating the value with ""
        return AttributeType.builder()
                .name(unescapeName(attr.getName()))
                .value("")
                .build();
    }

    /**
     * User Custom attribute has ":" in the name.
     * We need to replace it to other character because ":" is reserved as character in XML identifiers and
     * may be used only for binding XML Namespace word in the IDM side.
     * We choice "_" as the alternative character.
     *
     * @param name
     * @return
     */
    public static String escapeName(String name) {
        return name.replaceAll("^custom:", "custom_");
    }

    /**
     * Restore the escaped name to original cognito name.
     *
     * @param name
     * @return
     */
    public static String unescapeName(String name) {
        return name.replaceAll("^custom_", "custom:");
    }

    /**
     * Check cognito result if it returns unexpected error.
     *
     * @param result
     * @param apiName
     */
    public static void checkCognitoResult(CognitoIdentityProviderResponse result, String apiName) {
        int status = result.sdkHttpResponse().statusCode();
        if (status != 200) {
            throw new ConnectorException(String.format("Cognito returns unexpected error when calling \"%s\". status: %d", apiName, status));
        }
    }

    /**
     * Check if ALLOW_PARTIAL_ATTRIBUTE_VALUES == true.
     *
     * @param options
     * @return
     */
    public static boolean shouldReturnPartialAttributeValues(OperationOptions options) {
        // If the option isn't set from IDM, it may be null.
        return options.getAllowPartialAttributeValues() == Boolean.TRUE;
    }
}
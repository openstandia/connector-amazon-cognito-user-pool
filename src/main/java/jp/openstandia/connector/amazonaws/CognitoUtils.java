package jp.openstandia.connector.amazonaws;

import com.amazonaws.AmazonWebServiceResult;
import com.amazonaws.services.cognitoidp.model.AttributeType;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeInfo;
import org.identityconnectors.framework.common.objects.AttributeUtil;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Map;

public class CognitoUtils {

    public static ZonedDateTime toZoneDateTime(Date d) {
        Instant instant = d.toInstant();
        ZoneId zone = ZoneId.systemDefault();
        return ZonedDateTime.ofInstant(instant, zone);
    }

    public static ZonedDateTime toZoneDateTime(String yyyymmdd) {
        LocalDate date = LocalDate.parse(yyyymmdd);
        return date.atStartOfDay(ZoneId.systemDefault());
    }

    public static AttributeType toCognitoAttribute(Map<String, AttributeInfo> schema, Attribute attr) {
        return new AttributeType()
                .withName(unescapeName(attr.getName()))
                .withValue(toCognitoValue(schema, attr));
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
            ZonedDateTime date = (ZonedDateTime)AttributeUtil.getSingleValue(attr);
            return date.format(DateTimeFormatter.ISO_LOCAL_DATE);
        }
        if (attributeInfo.getType() == Boolean.class) {
            return AttributeUtil.getAsStringValue(attr);
        }

        return AttributeUtil.getAsStringValue(attr);
    }

    public static AttributeType toCognitoAttributeForDelete(Attribute attr) {
        // Cognito deletes the attribute when updating the value with ""
        return new AttributeType()
                .withName(unescapeName(attr.getName()))
                .withValue("");
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
    public static void checkCognitoResult(AmazonWebServiceResult result, String apiName) {
        int status = result.getSdkHttpMetadata().getHttpStatusCode();
        if (status != 200) {
            throw new ConnectorException(String.format("Cognito returns unexpected error when calling \"%s\". status: %d", apiName, status));
        }
    }
}

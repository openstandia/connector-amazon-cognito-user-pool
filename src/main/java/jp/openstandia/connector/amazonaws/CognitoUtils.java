package jp.openstandia.connector.amazonaws;

import com.amazonaws.services.cognitoidp.model.AttributeType;
import org.identityconnectors.framework.common.objects.Attribute;
import org.identityconnectors.framework.common.objects.AttributeUtil;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

public class CognitoUtils {

    public static ZonedDateTime toZoneDateTime(Date d) {
        Instant instant = d.toInstant();
        ZoneId zone = ZoneId.systemDefault();
        return ZonedDateTime.ofInstant(instant, zone);
    }

    public static AttributeType toAttributeType(Attribute attr) {
        return new AttributeType().withName(unescapeName(attr.getName())).withValue(AttributeUtil.getAsStringValue(attr));
    }

    public static AttributeType toAttributeTypeForDelete(Attribute attr) {
        // Cognito deletes the attribute when updating the value with ""
        return new AttributeType().withName(unescapeName(attr.getName())).withValue("");
    }

    /**
     * User Custom attribute has ":" in the name.
     * Because ":" is reserved as character in XML identifiers and may be used only for binding XML Namespace word,
     * we need to replace it to other valid character.
     * We choice "_" as the alternative character.
     *
     * @param name
     * @return
     */
    public static String escapeName(String name) {
        return name.replaceAll("^custom:", "custom_");
    }

    public static String unescapeName(String name) {
        return name.replaceAll("^custom_", "custom:");
    }
}

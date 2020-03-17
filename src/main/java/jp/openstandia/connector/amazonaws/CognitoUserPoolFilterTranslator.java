package jp.openstandia.connector.amazonaws;

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.AbstractFilterTranslator;
import org.identityconnectors.framework.common.objects.filter.EqualsFilter;
import org.identityconnectors.framework.common.objects.filter.StartsWithFilter;

public class CognitoUserPoolFilterTranslator extends AbstractFilterTranslator<CognitoUserPoolFilter> {

    private static final Log LOG = Log.getLog(CognitoUserPoolFilterTranslator.class);

    private final OperationOptions options;
    private final ObjectClass objectClass;

    public CognitoUserPoolFilterTranslator(ObjectClass objectClass, OperationOptions options) {
        this.objectClass = objectClass;
        this.options = options;
    }

    @Override
    protected CognitoUserPoolFilter createEqualsExpression(EqualsFilter filter, boolean not) {
        if (not) { // no way (natively) to search for "NotEquals"
            return null;
        }
        Attribute attr = filter.getAttribute();

        // Cognito doesn't support searching by custom attribute
        if (isCustomAttribute(attr)) {
            return null;
        }

        if (attr instanceof Uid) {
            Uid uid = (Uid) attr;
            Name nameHint = uid.getNameHint();
            if (nameHint != null) {
                CognitoUserPoolFilter nameFilter = new CognitoUserPoolFilter(nameHint.getName(),
                        CognitoUserPoolFilter.FilterType.EXACT_MATCH,
                        nameHint.getNameValue());
                return nameFilter;
            }
        }

        CognitoUserPoolFilter cognitoFilter = new CognitoUserPoolFilter(attr.getName(),
                CognitoUserPoolFilter.FilterType.EXACT_MATCH,
                AttributeUtil.getAsStringValue(attr));

        return cognitoFilter;
    }

    @Override
    protected CognitoUserPoolFilter createStartsWithExpression(StartsWithFilter filter, boolean not) {
        if (not) { // no way (natively) to search for "NotStartsWith"
            return null;
        }

        Attribute attr = filter.getAttribute();

        // Cognito doesn't support searching by custom attribute
        if (isCustomAttribute(attr)) {
            return null;
        }

        CognitoUserPoolFilter cognitoFilter = new CognitoUserPoolFilter(attr.getName(),
                CognitoUserPoolFilter.FilterType.PREFIX_MATCH,
                AttributeUtil.getAsStringValue(attr));

        return cognitoFilter;
    }

    private boolean isCustomAttribute(Attribute attr) {
        return attr.getName().startsWith("custom:");
    }
}

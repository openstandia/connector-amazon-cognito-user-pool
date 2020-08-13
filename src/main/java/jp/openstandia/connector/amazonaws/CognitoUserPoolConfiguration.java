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

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.ConfigurationException;
import org.identityconnectors.framework.spi.AbstractConfiguration;
import org.identityconnectors.framework.spi.ConfigurationProperty;
import software.amazon.awssdk.regions.Region;

public class CognitoUserPoolConfiguration extends AbstractConfiguration {

    private String userPoolID;
    private GuardedString awsAccessKeyID;
    private GuardedString awsSecretAccessKey;
    private String defaultRegion;
    private String httpProxyHost;
    private int httpProxyPort;
    private String httpProxyUser;
    private GuardedString httpProxyPassword;
    private boolean suppressInvitationMessageEnabled = true;

    @ConfigurationProperty(
            order = 1,
            displayMessageKey = "User Pool ID",
            helpMessageKey = "User Pool ID which is connected from this connector.",
            required = true,
            confidential = false)
    public String getUserPoolID() {
        return userPoolID;
    }

    public void setUserPoolID(String userPoolID) {
        this.userPoolID = userPoolID;
    }

    @ConfigurationProperty(
            order = 2,
            displayMessageKey = "AWS Access Key ID",
            helpMessageKey = "Set your AWS Access Key ID to connect Amazon Cognito. " +
                    "This option will be used when you want to deploy this connector in on-premises environment " +
                    "or to use testing purpose.",
            required = false,
            confidential = true)
    public GuardedString getAWSAccessKeyID() {
        return awsAccessKeyID;
    }

    public void setAWSAccessKeyID(GuardedString awsAccessKeyID) {
        this.awsAccessKeyID = awsAccessKeyID;
    }

    @ConfigurationProperty(
            order = 3,
            displayMessageKey = "AWS Secret Access Key",
            helpMessageKey = "Set your AWS Secret Access Key to connect Amazon Cognito. " +
                    "This option will be used when you want to deploy this connector in on-premises environment " +
                    "or to use testing purpose.",
            required = false,
            confidential = true)
    public GuardedString getAWSSecretAccessKey() {
        return awsSecretAccessKey;
    }

    public void setAWSSecretAccessKey(GuardedString awsSecretAccessKey) {
        this.awsSecretAccessKey = awsSecretAccessKey;
    }

    @ConfigurationProperty(
            order = 4,
            displayMessageKey = "Default Region",
            helpMessageKey = "Default Region",
            required = false,
            confidential = false)
    public String getDefaultRegion() {
        return defaultRegion;
    }

    public void setDefaultRegion(String defaultRegion) {
        this.defaultRegion = defaultRegion;
    }

    @ConfigurationProperty(
            order = 5,
            displayMessageKey = "HTTP Proxy Host",
            helpMessageKey = "Hostname for the HTTP Proxy",
            required = false,
            confidential = false)
    public String getHttpProxyHost() {
        return httpProxyHost;
    }

    public void setHttpProxyHost(String httpProxyHost) {
        this.httpProxyHost = httpProxyHost;
    }

    @ConfigurationProperty(
            order = 6,
            displayMessageKey = "HTTP Proxy Port",
            helpMessageKey = "Port for the HTTP Proxy",
            required = false,
            confidential = false)
    public int getHttpProxyPort() {
        return httpProxyPort;
    }

    public void setHttpProxyPort(int httpProxyPort) {
        this.httpProxyPort = httpProxyPort;
    }

    @ConfigurationProperty(
            order = 7,
            displayMessageKey = "HTTP Proxy User",
            helpMessageKey = "Username for the HTTP Proxy Authentication",
            required = false,
            confidential = false)
    public String getHttpProxyUser() {
        return httpProxyUser;
    }

    public void setHttpProxyUser(String httpProxyUser) {
        this.httpProxyUser = httpProxyUser;
    }

    @ConfigurationProperty(
            order = 8,
            displayMessageKey = "HTTP Proxy Password",
            helpMessageKey = "Password for the HTTP Proxy Authentication",
            required = false,
            confidential = true)
    public GuardedString getHttpProxyPassword() {
        return httpProxyPassword;
    }

    public void setHttpProxyPassword(GuardedString httpProxyPassword) {
        this.httpProxyPassword = httpProxyPassword;
    }

    @ConfigurationProperty(
            order = 9,
            displayMessageKey = "Suppress Invitation Message",
            helpMessageKey = "If enabled, suppress sending invitation message when creating the user. Default: true",
            required = false,
            confidential = false)
    public boolean isSuppressInvitationMessageEnabled() {
        return suppressInvitationMessageEnabled;
    }

    public void setSuppressInvitationMessageEnabled(boolean suppressInvitationMessageEnabled) {
        this.suppressInvitationMessageEnabled = suppressInvitationMessageEnabled;
    }

    @Override
    public void validate() {
        if (StringUtil.isNotEmpty(getDefaultRegion())) {
            try {
                Region.of(getDefaultRegion());
            } catch (IllegalArgumentException e) {
                throw new ConfigurationException("Invalid AWS Region name: " + getDefaultRegion());
            }
        }
    }
}

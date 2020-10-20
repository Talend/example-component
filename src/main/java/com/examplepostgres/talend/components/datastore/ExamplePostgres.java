package com.examplepostgres.talend.components.datastore;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

@DataStore("ExamplePostgres")
@GridLayout({
    // the generated layout put one configuration entry per line,
    // customize it as much as needed
    @GridLayout.Row({ "url" }),
    @GridLayout.Row({ "port" }),
    @GridLayout.Row({ "username" }),
    @GridLayout.Row({ "password" })
})
@Documentation("TODO fill the documentation for this configuration")
public class ExamplePostgres implements Serializable {
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String url;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private int port;

    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String username;

    @Credential
    @Option
    @Documentation("TODO fill the documentation for this parameter")
    private String password;

    public String getUrl() {
        return url;
    }

    public ExamplePostgres setUrl(String url) {
        this.url = url;
        return this;
    }

    public int getPort() {
        return port;
    }

    public ExamplePostgres setPort(int port) {
        this.port = port;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ExamplePostgres setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ExamplePostgres setPassword(String password) {
        this.password = password;
        return this;
    }
}
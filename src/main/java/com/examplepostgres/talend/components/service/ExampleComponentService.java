package com.examplepostgres.talend.components.service;

import com.examplepostgres.talend.components.datastore.ExamplePostgres;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

@Service
public class ExampleComponentService {

    @HealthCheck(value = "validateConnection")
	public HealthCheckStatus healthCheck(@Option ExamplePostgres conn)
	{
		try { 
			conn.getConnection();
			return new HealthCheckStatus(HealthCheckStatus.Status.OK, "Success");
		} catch(Exception e) {
			return new HealthCheckStatus(HealthCheckStatus.Status.KO, "Failed > " + e.getMessage());
		}
	}


    @Suggestions("showTables")
    public SuggestionValues listTables(@Option ExamplePostgres datastore)
    {
        SuggestionValues values = new SuggestionValues();
        values.setItems(datastore.getTables());

        return values;
    }
}
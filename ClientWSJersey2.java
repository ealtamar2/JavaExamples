package org.test;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Form;
import javax.ws.rs.core.MediaType;

import org.test.util.ServerUtils;


public class ExampleServiceClient {

	/**
	 * @param args
	 */		

	
    private WebTarget  webTarget;
    private Client client;
    private static final String BASE_URI = ServerUtils.ULR;

    public ExampleServiceClient() {
        client =ClientBuilder.newClient();
   
    }

    public String getRoute(String points,String index,String currentPosition) throws Exception {
       
        Form form = new Form();
        form.param("points",points);
        form.param("startpoint", index);
        form.param("currentposition", currentPosition);
        
        webTarget = client.target(BASE_URI).path("route");
        
        String result = webTarget.request(MediaType.APPLICATION_JSON_TYPE)
        		.post(Entity.entity(form, MediaType.APPLICATION_FORM_URLENCODED),String.class);
    
        return result;
   
}

    public void close() {
       // client.close();
    }
	

}
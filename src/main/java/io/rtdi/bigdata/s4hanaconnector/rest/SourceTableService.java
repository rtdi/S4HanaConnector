package io.rtdi.bigdata.s4hanaconnector.rest;

import java.util.List;

import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.ServletContext;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Configuration;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.rest.JAXBErrorResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.rest.JAXBSuccessResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.s4hanaconnector.S4HanaTableMapping;
import io.rtdi.bigdata.s4hanaconnector.S4HanaBrowse;
import io.rtdi.bigdata.s4hanaconnector.S4HanaBrowse.TableImport;
import io.rtdi.bigdata.s4hanaconnector.S4HanaConnectionProperties;

@Path("/")
public class SourceTableService {
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;

	public SourceTableService() {
	}
			
	@GET
	@Path("/connections/{connectionname}/sourcetables")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getFiles(@PathParam("connectionname") String connectionname, @PathParam("name") String name) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController connection = connector.getConnectionOrFail(connectionname);
			S4HanaBrowse browser = (S4HanaBrowse) connection.getBrowser();
			return Response.ok(browser.getHanaTables()).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	@POST
	@Path("/connections/{connectionname}/sourcetables")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getFiles(@PathParam("connectionname") String connectionname, List<TableImport> data) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController connection = connector.getConnectionOrFail(connectionname);
			S4HanaConnectionProperties props = (S4HanaConnectionProperties) connection.getConnectionProperties();
			String dbuser = props.getUsername();
			String dbschema = props.getSourceSchema();
			S4HanaBrowse browser = (S4HanaBrowse) connection.getBrowser();
			for (TableImport t : data) {
				S4HanaTableMapping entity = new S4HanaTableMapping(t.getSchemaname(), dbuser, dbschema, t.getTablename(), "L1", t.getInitialloadwhere(), browser.getConnection());
				entity.write(browser.getBusinessObjectDirectory());
			}
			return JAXBSuccessResponseBuilder.getJAXBResponse("Saved " + data.size() + " table schemas");
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

}
package io.rtdi.bigdata.s4hanaconnector.rest;

import java.util.List;

import javax.annotation.security.RolesAllowed;
import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Configuration;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import io.rtdi.bigdata.connector.connectorframework.WebAppController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectionController;
import io.rtdi.bigdata.connector.connectorframework.controller.ConnectorController;
import io.rtdi.bigdata.connector.connectorframework.rest.JAXBErrorResponseBuilder;
import io.rtdi.bigdata.connector.connectorframework.rest.entity.BrowsingNode;
import io.rtdi.bigdata.connector.connectorframework.servlet.ServletSecurityConstants;
import io.rtdi.bigdata.s4hanaconnector.S4HanaBrowse;

@Path("/")
public class SourceTreeService {
	@Context
    private Configuration configuration;

	@Context 
	private ServletContext servletContext;

	public SourceTreeService() {
	}
			
	@GET
	@Path("/connections/{connectionname}/sourcetree")
    @Produces(MediaType.APPLICATION_JSON)
	@RolesAllowed({ServletSecurityConstants.ROLE_VIEW})
    public Response getFiles(@PathParam("connectionname") String connectionname, @QueryParam("nodeid") String nodeid) {
		try {
			ConnectorController connector = WebAppController.getConnectorOrFail(servletContext);
			ConnectionController connection = connector.getConnectionOrFail(connectionname);
			S4HanaBrowse browser = (S4HanaBrowse) connection.getBrowser();
			List<BrowsingNode> data = browser.getHanaTableTree(nodeid);
			return Response.ok(new BrowserData(data)).build();
		} catch (Exception e) {
			return JAXBErrorResponseBuilder.getJAXBResponse(e);
		}
	}

	public static class BrowserData {
		private List<BrowsingNode> data;
		
		public BrowserData() {
		}

		public BrowserData(List<BrowsingNode> data) {
			this.data = data;
		}

		public List<BrowsingNode> getData() {
			return data;
		}

		public void setData(List<BrowsingNode> data) {
			this.data = data;
		}
		
		
	}
}
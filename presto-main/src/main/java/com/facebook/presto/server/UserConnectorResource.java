package com.facebook.presto.server;

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.discovery.client.Announcer;
import io.airlift.discovery.client.ServiceAnnouncement;
import io.airlift.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by frank on 2018/4/26.
 */
@Path("/v1/yr/connector")
public class UserConnectorResource
{
    ConnectorManager connectorManager;
    CatalogManager catalogManager;
    Announcer announcer;
    Injector injector;
    InternalNodeManager nodeManager;

    @Inject
    public UserConnectorResource(
            Injector injector,
            Announcer announcer,
            InternalNodeManager nodeManager,
            ConnectorManager connectorManager,
            CatalogManager catalogManager)
    {
        this.announcer = announcer;
        this.injector = injector;
        this.nodeManager = nodeManager;
        this.connectorManager = connectorManager;
        this.catalogManager = catalogManager;
    }

    @GET
    public List<String> getCatalogs()
    {
        ArrayList<String> catalogs = new ArrayList<>();
        for (Catalog catalog : this.catalogManager.getCatalogs()) {
            catalogs.add(catalog.getCatalogName());
        }
        return catalogs;
    }

    @DELETE
    @Path("{catalog}")
    public void removeCatalog(@PathParam("catalog") String catalog)
    {
        connectorManager.dropConnection(catalog);
        ConnectorId connectorId = new ConnectorId(catalog);
        this.remove(connectorId);
    }

    @POST
    public void addConnector(Map<String, String> properties)
    {
        String catalogName = properties.remove("connector.catalog");
        String connectorName = properties.remove("connector.name");
        ConnectorId connectorId = connectorManager.createConnection(catalogName, connectorName, ImmutableMap.copyOf(properties));
        this.add(connectorId);
    }

    private void add(ConnectorId connectorId)
    {
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.add(connectorId.toString());
        this.refreshNodes(announcement, properties, connectorIds);
    }

    private void remove(ConnectorId connectorId)
    {
        ServiceAnnouncement announcement = getPrestoAnnouncement(announcer.getServiceAnnouncements());
        Map<String, String> properties = new LinkedHashMap<>(announcement.getProperties());
        String property = nullToEmpty(properties.get("connectorIds"));
        Set<String> connectorIds = new LinkedHashSet<>(Splitter.on(',').trimResults().omitEmptyStrings().splitToList(property));
        connectorIds.remove(connectorId.toString());
        this.refreshNodes(announcement, properties, connectorIds);
    }

    private void refreshNodes(ServiceAnnouncement announcement, Map<String, String> properties, Set<String> connectorIds)
    {
        properties.put("connectorIds", Joiner.on(',').join(connectorIds));
        announcer.removeServiceAnnouncement(announcement.getId());
        announcer.addServiceAnnouncement(serviceAnnouncement(announcement.getType()).addProperties(properties).build());
        announcer.forceAnnounce();
        nodeManager.refreshNodes();
    }

    private static ServiceAnnouncement getPrestoAnnouncement(Set<ServiceAnnouncement> announcements)
    {
        for (ServiceAnnouncement announcement : announcements) {
            if (announcement.getType().equals("presto")) {
                return announcement;
            }
        }
        throw new RuntimeException("Presto announcement not found: " + announcements);
    }
}

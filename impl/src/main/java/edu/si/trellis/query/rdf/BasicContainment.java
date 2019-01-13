package edu.si.trellis.query.rdf;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

import edu.si.trellis.RdfReadConsistency;

import javax.inject.Inject;

import org.apache.commons.rdf.api.IRI;

public class BasicContainment extends ResourceQuery {

    @Inject
    public BasicContainment(Session session, @RdfReadConsistency ConsistencyLevel consistency) {
        super(session, "SELECT identifier AS contained FROM " + BASIC_CONTAINMENT_TABLENAME + " WHERE container = ? ;",
                        consistency);
    }

    public ResultSet execute(IRI id) {
        return executeSyncRead(preparedStatement().bind(id));
    }
}

package com.bah.culvert.accumulo.database;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.Connector;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

/**
 * Test the table adapter for simple things.
 */
public class TestTableAdapter {

  /**
   * Regression test to make sure names are set
   * @throws Exception
   */
  @Test
  public void testGetTableNames() throws Exception {
    Connector mockConnector = PowerMock.createMock(Connector.class);
    long memory = 1;
    long latency = 2;
    int threads = 3;
    BatchWriter mockWriter = PowerMock.createMock(BatchWriter.class);
    expect(
        mockConnector.createBatchWriter("tableName", memory, latency, threads))
        .andReturn(mockWriter);
    
    PowerMock.replayAll();
    AccumuloTableAdapter table = new AccumuloTableAdapter(mockConnector,
        "tableName", memory, latency, threads);
    assertEquals("tableName", table.getTableName());
    verifyAll();
  }
}

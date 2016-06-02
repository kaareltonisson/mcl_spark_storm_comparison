package sparkStormBenchmark.tests.simpleString;

import org.apache.storm.topology.TopologyBuilder;

import sparkStormBenchmark.StormCore;

public class StormSimpleString extends StormCore{

	public StormSimpleString(String host, int port) {
		super(host, port);
	}


	/**
	 * Creates the topology.
	 * @param host
	 * @param port
	 * @return
	 */
	public TopologyBuilder createTopology(String host, int port){
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("textSpout1", new KafkaSpoutSimpleString(host, port), 1);
		builder.setBolt("textBolt1", new TextBoltSimpleString(), 2)
			.shuffleGrouping("textSpout1");
		builder.setBolt("outputBolt", new OutputBolt(), 1)
			.shuffleGrouping("textBolt1");
		return builder;
	}
	
	/**
	 * Calls topology creation and starts the topology.
	 */
	public void createAndRunTopology(){
		TopologyBuilder builder = createTopology(this.host,this.port);
		String topologyName = "simpleStringTopology";
		startTopology(builder, topologyName);	
	}
}

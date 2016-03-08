/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.caseystella.analytics.integration.components;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.caseystella.analytics.integration.InMemoryComponent;
import com.caseystella.analytics.integration.UnableToStartException;
import org.apache.storm.flux.FluxBuilder;
import org.apache.storm.flux.model.ExecutionContext;
import org.apache.storm.flux.model.TopologyDef;
import org.apache.storm.flux.parser.FluxParser;
import org.apache.thrift7.TException;
import org.junit.Assert;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

public class StormTopologyComponent implements InMemoryComponent {
    LocalCluster stormCluster;
    String topologyName;
    File topologyLocation;
    Properties topologyProperties;
    TopologyBuilder topologyBuilder;
    Config stormConfig;
    public static class Builder {
        String topologyName;
        File fluxLocation;
        Properties topologyProperties;
        TopologyBuilder topology;
        Config config;
        public Builder withTopology(TopologyBuilder topology) {
            return withTopology(topology, new Config());
        }
        public Builder withTopology(TopologyBuilder topology, Config config) {
            this.topology = topology;
            this.config = config;
            return this;
        }
        public Builder withTopologyName(String name) {
            this.topologyName = name;
            return this;
        }
        public Builder withFluxLocation(File location, Properties properties) {
            this.fluxLocation = location;
            this.topologyProperties = properties;
            return this;
        }

        public StormTopologyComponent build() {
            return new StormTopologyComponent(topologyName, fluxLocation, topologyProperties);
        }
    }

    StormTopologyComponent(String topologyName, TopologyBuilder builder, Config config) {
        this.topologyBuilder = builder;
        this.topologyName = topologyName;
        this.stormConfig = config;
    }
    StormTopologyComponent(String topologyName, File topologyLocation, Properties topologyProperties) {
        this.topologyName = topologyName;
        this.topologyLocation = topologyLocation;
        this.topologyProperties = topologyProperties;
    }

    public LocalCluster getStormCluster() {
        return stormCluster;
    }

    public String getTopologyName() {
        return topologyName;
    }

    public File getTopologyLocation() {
        return topologyLocation;
    }

    public Properties getTopologyProperties() {
        return topologyProperties;
    }

    public Config getStormConfig() {
        return stormConfig;
    }

    public void start() throws UnableToStartException {
        try {
            stormCluster = new LocalCluster();
        } catch (Exception e) {
            throw new UnableToStartException("Unable to start flux topology: " + getTopologyLocation(), e);
        }
    }

    public void stop() {
        stormCluster.shutdown();
    }

    public void submitTopology() throws NoSuchMethodException, IOException, InstantiationException, TException, IllegalAccessException, InvocationTargetException, ClassNotFoundException {
        if(topologyBuilder == null) {
            submitFluxTopology(getTopologyName(), getTopologyLocation(), getTopologyProperties());
        }
        else {
            submitTopology(getTopologyName(), topologyBuilder, stormConfig);
        }
    }

    public void submitTopology( TopologyBuilder builder, Config config) {
        submitTopology(getTopologyName(), builder, config);
    }
    public void submitTopology(String topologyName, TopologyBuilder builder, Config config) {
        stormCluster.submitTopology(topologyName, config, builder.createTopology());
    }
    public void submitFluxTopology(String topologyName, File topologyLoc, Properties properties) throws IOException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException, TException {
        TopologyDef topologyDef = loadYaml(topologyName, topologyLoc, properties);
        Config conf = FluxBuilder.buildConfig(topologyDef);
        ExecutionContext context = new ExecutionContext(topologyDef, conf);
        StormTopology topology = FluxBuilder.buildTopology(context);
        Assert.assertNotNull(topology);
        topology.validate();
        stormCluster.submitTopology(topologyName, conf, topology);
    }

    private static TopologyDef loadYaml(String topologyName, File yamlFile, Properties properties) throws IOException {
        File tmpFile = File.createTempFile(topologyName, "props");
        tmpFile.deleteOnExit();
        FileWriter propWriter = null;
        try {
            propWriter = new FileWriter(tmpFile);
            properties.store(propWriter, topologyName + " properties");
        }
        finally {
            if(propWriter != null) {
                propWriter.close();
                return FluxParser.parseFile(yamlFile.getAbsolutePath(), false, true, tmpFile.getAbsolutePath(), false);
            }

            return null;
        }
    }
}

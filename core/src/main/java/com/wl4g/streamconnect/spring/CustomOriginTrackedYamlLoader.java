/*
 *  Copyright (C) 2023 ~ 2035 the original authors WL4G (James Wong).
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.wl4g.streamconnect.spring;

import com.wl4g.streamconnect.config.StreamConnectYamlConstructor;
import org.springframework.beans.factory.config.YamlProcessor;
import org.springframework.boot.context.properties.bind.PlaceholdersResolver;
import org.springframework.boot.context.properties.bind.PropertySourcesPlaceholdersResolver;
import org.springframework.boot.origin.Origin;
import org.springframework.boot.origin.OriginTrackedValue;
import org.springframework.boot.origin.TextResourceOrigin;
import org.springframework.boot.origin.TextResourceOrigin.Location;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.Resource;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.BaseConstructor;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.extensions.compactnotation.CompactConstructor;
import org.yaml.snakeyaml.nodes.CollectionNode;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;
import org.yaml.snakeyaml.resolver.Resolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Class to load {@code .yml} files into a map of {@code String} to
 * {@link OriginTrackedValue}.
 *
 * @author Madhura Bhave
 * @author Phillip Webb
 */
class CustomOriginTrackedYamlLoader extends YamlProcessor {

    private final Resource resource;

    CustomOriginTrackedYamlLoader(Resource resource) {
        this.resource = resource;
        setResources(resource);
    }

    @Override
    protected Yaml createYaml() {
        LoaderOptions loaderOptions = new LoaderOptions();
        loaderOptions.setAllowDuplicateKeys(false);
        loaderOptions.setMaxAliasesForCollections(Integer.MAX_VALUE);
        loaderOptions.setAllowRecursiveKeys(true);
        return createYaml(loaderOptions);
    }

    private Yaml createYaml(LoaderOptions loaderOptions) {
        BaseConstructor constructor = new OriginTrackingConstructor(loaderOptions);
        Representer representer = new Representer();
        DumperOptions dumperOptions = new DumperOptions();
        LimitedResolver resolver = new LimitedResolver();
        return new Yaml(constructor, representer, dumperOptions, loaderOptions, resolver);
    }

    List<Map<String, Object>> load() {
        final List<Map<String, Object>> result = new ArrayList<>();
        process((properties, map) -> result.add(getFlattenedMap(map)));
        return result;
    }

    //
    // [Begin] MODIFY FEATURE.
    //
    private class OriginTrackingConstructor extends /*SafeConstructor*/ CompactConstructor {
        private final PlaceholdersResolver resolver = new PropertySourcesPlaceholdersResolver(new StandardEnvironment());
        //
        // [End] MODIFY FEATURE.
        //

        OriginTrackingConstructor(LoaderOptions loadingConfig) {
            super();
            StreamConnectYamlConstructor.configure(this);
        }

        @Override
        public Object getData() throws NoSuchElementException {
            Object data = super.getData();
            if (data instanceof CharSequence && ((CharSequence) data).length() == 0) {
                return null;
            }
            return data;
        }

        @Override
        protected Object constructObject(Node node) {
            if (node instanceof CollectionNode && ((CollectionNode<?>) node).getValue().isEmpty()) {
                return constructTrackedObject(node, super.constructObject(node));
            }
            if (node instanceof ScalarNode) {
                if (!(node instanceof KeyScalarNode)) {
                    return constructTrackedObject(node, super.constructObject(node));
                }
            }
            if (node instanceof MappingNode) {
                replaceMappingNodeKeys((MappingNode) node);
            }
            return super.constructObject(node);
        }

        private void replaceMappingNodeKeys(MappingNode node) {
            node.setValue(node.getValue().stream().map(KeyScalarNode::get).collect(Collectors.toList()));
        }

        @SuppressWarnings("unused")
        private Object constructTrackedObject(Node node, Object value) {
            //
            // [Begin] REMOVE FEATURES.
            // 使用 OriginTrackedValue 序列化和反序列化时对多态类型的处理会有问题.
            //
            Origin origin = getOrigin(node);
            // return OriginTrackedValue.of(getValue(value), origin);
            //
            // [End] REMOVE FEATURES.
            //

            //
            // [Begin] ADD FEATURES.
            //
            return getValue(value);
            //
            // [End] ADD FEATURES.
            //
        }

        private Object getValue(Object value) {
            //
            // [Begin] MODIFY FEATURE. 为解决改用 CompactConstructor 后支持了多态解析，但又无法按 env 解析的问题.
            //
            if (value instanceof String) {
                Object resolvedValue = resolver.resolvePlaceholders(value.toString());
                return (resolvedValue != null) ? resolvedValue : "";
            }
            //
            // [End] MODIFY FEATURE.
            //
            return value;
        }

        private Origin getOrigin(Node node) {
            Mark mark = node.getStartMark();
            Location location = new Location(mark.getLine(), mark.getColumn());
            return new TextResourceOrigin(CustomOriginTrackedYamlLoader.this.resource, location);
        }

    }

    /**
     * {@link ScalarNode} that replaces the key node in a {@link NodeTuple}.
     */
    private static class KeyScalarNode extends ScalarNode {

        KeyScalarNode(ScalarNode node) {
            super(node.getTag(), node.getValue(), node.getStartMark(), node.getEndMark(), node.getScalarStyle());
        }

        static NodeTuple get(NodeTuple nodeTuple) {
            Node keyNode = nodeTuple.getKeyNode();
            Node valueNode = nodeTuple.getValueNode();
            return new NodeTuple(KeyScalarNode.get(keyNode), valueNode);
        }

        private static Node get(Node node) {
            if (node instanceof ScalarNode) {
                return new KeyScalarNode((ScalarNode) node);
            }
            return node;
        }

    }

    /**
     * {@link Resolver} that limits {@link Tag#TIMESTAMP} tags.
     */
    private static class LimitedResolver extends Resolver {

        @Override
        public void addImplicitResolver(Tag tag, Pattern regexp, String first) {
            if (tag == Tag.TIMESTAMP) {
                return;
            }
            super.addImplicitResolver(tag, regexp, first);
        }

    }

}
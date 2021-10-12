/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
 * with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package ai.djl.repository;

import ai.djl.repository.zoo.ModelLoader;
import ai.djl.repository.zoo.ModelZoo;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RepositoryFactoryImpl implements RepositoryFactory {

    private static final Logger logger = LoggerFactory.getLogger(RepositoryFactoryImpl.class);

    private static final Pattern NAME_PATTERN = Pattern.compile("model_name=([^&]*)");
    private static final Pattern ARTIFACT_PATTERN = Pattern.compile("artifact_id=([^&]*)");
    private static final RepositoryFactory FACTORY = new RepositoryFactoryImpl();
    private static final Map<String, RepositoryFactory> REGISTRY = init();

    static RepositoryFactory getFactory() {
        return FACTORY;
    }

    /** {@inheritDoc} */
    @Override
    public Repository newInstance(String name, URI uri) {
        String scheme = uri.getScheme();
        if (scheme == null) {
            scheme = "file";
        }
        RepositoryFactory factory = REGISTRY.get(scheme);
        if (factory != null) {
            return factory.newInstance(name, uri);
        }

        try {
            uri.toURL();
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Malformed URL: " + uri, e);
        }

        Path path = parseFilePath(uri);
        String fileName = path.toFile().getName();
        if (FilenameUtils.isArchiveFile(fileName)) {
            String[] names = parseQueryString(uri, fileName);
            return new SimpleUrlRepository(name, uri, names[0], names[1]);
        }
        return new RemoteRepository(name, uri);
    }

    /** {@inheritDoc} */
    @Override
    public Set<String> getSupportedScheme() {
        return Collections.emptySet();
    }

    static void registerRepositoryFactory(RepositoryFactory factory) {
        for (String scheme : factory.getSupportedScheme()) {
            REGISTRY.put(scheme, factory);
        }
    }

    private static Map<String, RepositoryFactory> init() {
        Map<String, RepositoryFactory> registry = new ConcurrentHashMap<>();
        registry.put("file", new LocalRepositoryFactory());
        registry.put("jar", new JarRepositoryFactory());
        registry.put("djl", new DjlRepositoryFactory());

        ServiceLoader<RepositoryFactory> factories = ServiceLoader.load(RepositoryFactory.class);
        for (RepositoryFactory factory : factories) {
            for (String scheme : factory.getSupportedScheme()) {
                registry.put(scheme, factory);
            }
        }
        return registry;
    }

    private static String[] parseQueryString(URI uri, String fileName) {
        String modelName = null;
        String artifactId = null;
        String query = uri.getQuery();
        if (query != null) {
            Matcher matcher = NAME_PATTERN.matcher(query);
            if (matcher.find()) {
                modelName = matcher.group(1);
            }
            matcher = ARTIFACT_PATTERN.matcher(query);
            if (matcher.find()) {
                artifactId = matcher.group(1);
            }
        }

        if (artifactId == null) {
            artifactId = FilenameUtils.getNamePart(fileName);
        }
        if (modelName == null) {
            modelName = artifactId;
        }
        return new String[] {artifactId, modelName};
    }

    static Path parseFilePath(URI uri) {
        String uriPath = uri.getPath();
        if (uriPath == null) {
            uriPath = uri.getSchemeSpecificPart();
        }
        if (uriPath.startsWith("/") && System.getProperty("os.name").startsWith("Win")) {
            uriPath = uriPath.substring(1);
        }
        return Paths.get(uriPath);
    }

    private static final class JarRepositoryFactory implements RepositoryFactory {

        /** {@inheritDoc} */
        @Override
        public Repository newInstance(String name, URI uri) {
            String p = uri.getPath();
            if (p.startsWith("/")) {
                p = p.substring(1);
            }
            URL u = Thread.currentThread().getContextClassLoader().getResource(p);
            if (u == null) {
                throw new IllegalArgumentException("Resource not found: " + uri);
            }
            try {
                uri = u.toURI();
            } catch (URISyntaxException e) {
                throw new IllegalArgumentException("Resource not found: " + uri, e);
            }

            Path path = parseFilePath(uri);
            String fileName = path.toFile().getName();
            if (!FilenameUtils.isArchiveFile(fileName)) {
                throw new IllegalArgumentException("Only archive file is supported for res URL.");
            }

            String[] names = parseQueryString(uri, fileName);
            return new JarRepository(name, uri, names[0], names[1]);
        }

        /** {@inheritDoc} */
        @Override
        public Set<String> getSupportedScheme() {
            return Collections.singleton("jar");
        }
    }

    private static final class LocalRepositoryFactory implements RepositoryFactory {

        /** {@inheritDoc} */
        @Override
        public Repository newInstance(String name, URI uri) {
            Path path = parseFilePath(uri);
            String fileName = path.toFile().getName();
            String[] names = parseQueryString(uri, fileName);
            if (Files.exists(path) && Files.isDirectory(path)) {
                try {
                    if (Files.walk(path)
                            .anyMatch(
                                    f ->
                                            f.endsWith("metadata.json")
                                                    && Files.isRegularFile(f)
                                                    && !f.getParent().equals(path))) {
                        logger.debug("Found local repository: {}", path);
                        return new LocalRepository(name, path);
                    }
                } catch (IOException e) {
                    logger.warn("Failed locate metadata.json file, defaulting to simple", e);
                }
            }
            return new SimpleRepository(name, path, names[0], names[1]);
        }

        /** {@inheritDoc} */
        @Override
        public Set<String> getSupportedScheme() {
            return Collections.singleton("file");
        }
    }

    private static final class DjlRepositoryFactory implements RepositoryFactory {

        /** {@inheritDoc} */
        @Override
        public Repository newInstance(String name, URI uri) {
            RemoteRepository repo =
                    new RemoteRepository(name, URI.create("https://mlrepo.djl.ai/"));
            String groupId = uri.getHost();
            Path path = parseFilePath(uri);
            int size = path.getNameCount();
            if (size == 0) {
                throw new IllegalArgumentException("Invalid djl URL: " + uri);
            }
            String artifactId = path.getName(0).toString();
            if (artifactId.isEmpty()) {
                throw new IllegalArgumentException("Invalid djl URL: " + uri);
            }
            String version = size > 1 ? path.getName(1).toString() : null;
            String artifactName = size > 2 ? path.getName(2).toString() : null;

            ModelZoo zoo = ModelZoo.getModelZoo(groupId);
            if (zoo == null) {
                logger.warn("ModelZoo not found: {}", groupId);
                return repo;
            }

            ModelLoader loader = zoo.getModelLoader(artifactId);
            if (loader == null) {
                logger.warn("Artifact not found: {}/{}", groupId, artifactId);
                return repo;
            }

            MRL mrl =
                    repo.model(loader.getApplication(), groupId, artifactId, version, artifactName);
            repo.addResource(mrl);
            return repo;
        }

        /** {@inheritDoc} */
        @Override
        public Set<String> getSupportedScheme() {
            return Collections.singleton("djl");
        }
    }
}

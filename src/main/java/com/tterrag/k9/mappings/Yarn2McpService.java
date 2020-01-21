package com.tterrag.k9.mappings;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.tterrag.k9.mappings.mcp.IntermediateMapping;
import com.tterrag.k9.mappings.mcp.McpDownloader;
import com.tterrag.k9.mappings.mcp.McpMapping;
import com.tterrag.k9.mappings.mcp.McpMapping.Side;
import com.tterrag.k9.mappings.mcp.SrgDatabase;
import com.tterrag.k9.mappings.mcp.SrgMapping;
import com.tterrag.k9.mappings.yarn.YarnDownloader;
import com.tterrag.k9.util.Monos;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple2;

@Slf4j
public class Yarn2McpService {
    
    private static final MappingType[] SUPPORTED_TYPES = { MappingType.FIELD, MappingType.METHOD };
    
    private static final String YARN = "yarn", MIXED = "mixed";
    private static final String MIXED_VERSION = "1.14.3";
    
    private static final String EOL = "\r\n";
    
    private static final String POM_TEMPLATE = 
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + EOL + 
            "<project xsi:schemaLocation=\"http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd\" xmlns=\"http://maven.apache.org/POM/4.0.0\"" + EOL + 
            "    xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">" + EOL + 
            "  <modelVersion>4.0.0</modelVersion>" + EOL + 
            "  <groupId>de.oceanlabs.mcp</groupId>" + EOL + 
            "  <artifactId>%1$s</artifactId>" + EOL + 
            "  <version>%2$s</version>" + EOL + 
            "</project>" + EOL;
    
    private static final ImmutableMap<String, String> CORRECTIONS = ImmutableMap.<String, String>builder()
            .put("func_213454_em", "wakeUpInternal") // FoxEntity
            .put("field_77356_a ", "protectionType") // ProtectionEnchantment
            .put("func_225486_c", "getTemperatureCached") // Biome
            .put("func_227833_h_", "resetUnchecked") // BufferBuilder
            .put("func_227480_b_", "findPathNodeType") // WalkNodeProcessor
            .put("func_228676_c_", "create") // RenderType$Type
            .build();
    
    public final Path output;
    
    public Yarn2McpService(String output) {
        this.output = Paths.get(output, "de", "oceanlabs", "mcp");
    }
    
    public Mono<Void> start() {
        // First, generate mappings for the latest mcp version if they are not done already.
        // These either shouldn't change (if yarn is on a newer version) or will get updated
        // by the daily export (if yarn is on the same version).
        return McpDownloader.INSTANCE.getLatestMinecraftVersion(true)
                .publishOn(Schedulers.elastic())
                .flatMap(version -> publishIfNotExists(version, true, YARN, this::publishMappings))
                // Then run initial output of the latest yarn version if no file exists for today
                .then(YarnDownloader.INSTANCE.getLatestMinecraftVersion(true))
                .flatMap(version -> publishIfNotExists(version, false, YARN, this::publishMappings).thenReturn(version))
                // Also publish mixed mappings
                .flatMap(version -> publishIfNotExists(version, false, MIXED, (v, $) -> publishMixedMappings(MIXED_VERSION, v)))
                // Then begin a periodic publish every day at midnight
                .thenMany(Flux.interval(Duration.between(Instant.now(), Instant.now().plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS)), Duration.ofDays(1)))
                .flatMap(tick -> YarnDownloader.INSTANCE.getLatestMinecraftVersion(true)
                        .publishOn(Schedulers.elastic())
                        .flatMap(version -> publishMappings(version, true).thenReturn(version))
                        .flatMap(version -> publishMixedMappings(MIXED_VERSION, version))
                        .thenReturn(tick)
                        .doOnError(t -> log.error("Error performing periodic mapping push", t))
                        .onErrorReturn(tick))
                .then();
    }
    
    private Mono<Void> publishIfNotExists(String version, boolean stable, String name, BiFunction<String, Boolean, Mono<Void>> func) {
        return Mono.fromSupplier(() -> getOutputFile(version, stable, name))
            .filter(p -> !p.toFile().exists())
            .flatMap($ -> func.apply(version, stable));
    }
    
    private Mono<SrgDatabase> getSrgs(String version) {
        return McpDownloader.INSTANCE.updateSrgs(version)
                .then(Mono.fromCallable(() -> (SrgDatabase) new SrgDatabase(version).reload()));
    }
    
    private Mono<Void> publishMixedMappings(String mcpVersion, String yarnVersion) {
        return Mono.zip(getSrgs(yarnVersion),
                        YarnDownloader.INSTANCE.getDatabase(yarnVersion),
                        McpDownloader.INSTANCE.getDatabase(mcpVersion))
                .as(Monos.groupWith(Flux.just(SUPPORTED_TYPES), (type, dbs) -> Mono.zip(
                        findMatching(type, dbs.getT1(), dbs.getT2()),
                        this.<SrgMapping, Mapping>findMatchingByIntermediate(type, dbs.getT1(), dbs.getT3()))))
                .flatMap(gf -> gf
                        .doOnNext(maps -> maps.getT1().forEach(maps.getT2()::putIfAbsent))
                        .map(Tuple2::getT2)
                        .flatMapIterable(Map::entrySet)
                        .filter(e -> e.getKey().getName() != null || e.getValue().getName() != null)
                        .map(e -> toCsv(e.getKey(), e.getValue()))
                        .collectList()
                        .flatMap(csv -> writeToFile(yarnVersion, false, gf.key(), csv, MIXED)))
                .then();
    }
    
    private Mono<Void> publishMappings(String version, boolean stable) {
        return Mono.zip(getSrgs(version), YarnDownloader.INSTANCE.getDatabase(version))
                .doOnNext($ -> log.info("Publishing yarn-over-mcp for MC " + version))
                .as(Monos.groupWith(Flux.just(SUPPORTED_TYPES), (type, t) -> findMatching(type, t.getT1(), t.getT2())))
                .flatMap(gf -> gf.flatMapIterable(m -> m.entrySet())
                        .filter(e -> e.getKey().getName() != null || e.getValue().getName() != null)
                        .map(e -> toCsv(e.getKey(), e.getValue()))
                        .collectList()
                        .flatMap(csv -> writeToFile(version, stable, gf.key(), csv, YARN)))
                .then();
    }

    private <M extends IntermediateMapping> Comparator<M> bySrg() {
        return Comparator.<M>comparingInt(m -> FastIntLookupDatabase.getSrgId(m.getIntermediate())
                .orElse(Integer.MAX_VALUE))
                .thenComparing(Mapping::getIntermediate);
    }
    
    private <M1 extends IntermediateMapping, M2 extends Mapping> Mono<SortedMap<M1, M2>> findMatchingByIntermediate(
            MappingType type, AbstractMappingDatabase<? extends M1> a, AbstractMappingDatabase<? extends M2> b) {
        return Mono.fromSupplier(() -> {
            ListMultimap<String, ? extends M1> aBySrg = a.getTable(NameType.INTERMEDIATE, type);
            ListMultimap<String, ? extends M2> bBySrg = b.getTable(NameType.INTERMEDIATE, type);
            SortedMap<M1, M2> ret = new TreeMap<>(bySrg());
            for (String key : aBySrg.keySet()) {
                List<? extends M1> aMappings = aBySrg.get(key);
                List<? extends M2> bMappings = bBySrg.get(key);
                if (!aMappings.isEmpty() && !bMappings.isEmpty()) {
                    ret.put(aMappings.get(0), bMappings.get(0));
                }
            }
            return ret;
        });
    }

    private <M1 extends IntermediateMapping, M2 extends Mapping> Mono<SortedMap<M1, M2>> findMatching(
            MappingType type, AbstractMappingDatabase<M1> mcp, AbstractMappingDatabase<M2> yarn) {
        return Mono.defer(() -> {
            Map<String, M1> mcpBySignature = bySignature(mcp.getTable(NameType.ORIGINAL, type));
            Map<String, M2> yarnBySignature = bySignature(yarn.getTable(NameType.ORIGINAL, type));
            return Flux.fromIterable(mcpBySignature.entrySet())
                    .filter(e -> yarnBySignature.containsKey(e.getKey()))
                    .collectMap(Map.Entry::getValue, e -> yarnBySignature.get(e.getKey()), () -> new TreeMap<M1, M2>(bySrg()))
                    .map(m -> (SortedMap<M1, M2>) m); // wtf reactor?
        });
    }
    
    private <M1 extends Mapping, M2 extends Mapping> String toCsv(M1 m1, M2 m2) {
        String name = m2.getName();
        if (name == null) {
            name = m1.getName();
        } else {
            name = CORRECTIONS.getOrDefault(m1.getIntermediate(), name);
        }
        return Joiner.on(',').join(
                m1.getIntermediate(),
                name,
                m1 instanceof McpMapping ? ((McpMapping)m1).getSide().ordinal() : Side.BOTH.ordinal(),
                Strings.nullToEmpty(m2.getName() != null ? getComment(m2) : getComment(m1)));
    }
    
    private String getComment(Mapping m) {
        if (m instanceof CommentedMapping) {
            return ((CommentedMapping) m).getComment();
        }
        return null;
    }
    
    private Path getOutputFile(String version, boolean stable, String name) {
        String channel = stable ? "mcp_stable" : "mcp_snapshot";
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        String snapshot = stable ? name + "-" + version : String.format("%04d%02d%02d-%s-%s", now.getYear(), now.getMonthValue(), now.getDayOfMonth(), name, version);
        return output
                .resolve(channel)
                .resolve(snapshot)
                .resolve(channel + "-" + snapshot + ".zip");
    }
    
    private Mono<Void> writeToFile(String version, boolean stable, MappingType type, List<String> csv, String name) {
        return Mono.fromRunnable(() -> {
            try {
                Path zip = getOutputFile(version, stable, name);
                File parent = zip.getParent().toFile();
                if (!parent.mkdirs() && !parent.exists()) {
                    throw new IllegalStateException("Could not create maven directory: " + zip.getParent());
                }
                Map<String, String> env = new HashMap<>(); 
                env.put("create", "true");
                URI uri = URI.create("jar:" + zip.toUri());
                log.info("Writing yarn-to-mcp data to " + zip.toAbsolutePath());
                String text = "searge,name,side,desc" + EOL + csv.stream().collect(Collectors.joining(EOL));
                try (FileSystem fs = FileSystems.newFileSystem(uri, env)) {
                    write(fs.getPath(type.getCsvName() + ".csv"), text);
                }
                if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
                    Files.setPosixFilePermissions(zip, PosixFilePermissions.fromString("rw-rw-r--"));
                }
                writeHashes(zip, text);
                Path pom = zip.getParent().resolve(zip.getFileName().toString().replace(".zip", ".pom"));
                String filename = pom.getFileName().toString().replace(".pom", "");
                int split = filename.indexOf('-');
                Object[] args = { filename.substring(0, split), filename.substring(split + 1) };
                String pomText = String.format(POM_TEMPLATE, args);
                write(pom, pomText);
                writeHashes(pom, pomText);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    private void write(Path path, String text) throws IOException {
        try (Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            writer.write(text);
        }
    }
    
    @SuppressWarnings("deprecation")
    private void writeHashes(Path base, String toHash) throws IOException {
        writeHash(base.getParent().resolve(base.getFileName().toString() + ".md5"), toHash, Hashing.md5());
        writeHash(base.getParent().resolve(base.getFileName().toString() + ".sha1"), toHash, Hashing.sha1());
    }
    
    private void writeHash(Path path, String text, HashFunction hasher) throws IOException {
        try (SeekableByteChannel chan = Files.newByteChannel(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            chan.write(ByteBuffer.wrap(hasher.hashString(text, StandardCharsets.UTF_8).asBytes()));
        }
    }
    
    private String getSignature(Mapping mapping) {
        StringBuilder ret = new StringBuilder(mapping.getOwner(NameType.ORIGINAL)).append(".").append(mapping.getOriginal());
        if (mapping.getType() == MappingType.METHOD) {
            ret.append(mapping.getDesc(NameType.ORIGINAL));
        }
        return ret.toString();
    }
    
    private <T extends Mapping> Map<String, T> bySignature(ListMultimap<String, T> table) {
        return table.values().stream()
                .collect(Collectors.toMap(this::getSignature, Function.identity()));
    }
}

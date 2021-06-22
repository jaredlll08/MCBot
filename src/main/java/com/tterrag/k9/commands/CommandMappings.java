package com.tterrag.k9.commands;

import java.io.File;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.tterrag.k9.commands.api.Argument;
import com.tterrag.k9.commands.api.CommandContext;
import com.tterrag.k9.commands.api.CommandPersisted;
import com.tterrag.k9.commands.api.Flag;
import com.tterrag.k9.commands.api.ICommand;
import com.tterrag.k9.commands.api.ReadyContext;
import com.tterrag.k9.mappings.Mapping;
import com.tterrag.k9.mappings.MappingDatabase;
import com.tterrag.k9.mappings.MappingDownloader;
import com.tterrag.k9.mappings.MappingType;
import com.tterrag.k9.util.GuildStorage;
import com.tterrag.k9.util.ListMessageBuilder;
import com.tterrag.k9.util.Monos;
import com.tterrag.k9.util.NullHelper;
import com.tterrag.k9.util.PaginatedMessageFactory.PaginatedMessage;
import com.tterrag.k9.util.Requirements;
import com.tterrag.k9.util.Requirements.RequiredType;
import com.tterrag.k9.util.annotation.NonNull;

import com.tterrag.k9.util.annotation.Nullable;
import discord4j.rest.util.Permission;
import lombok.Getter;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class CommandMappings<@NonNull M extends Mapping> extends CommandPersisted<String> {

    public static final Map<String, CommandMappings<?>> MAPPINGS_MAP = new HashMap<>();

    protected static final Argument<String> ARG_NAME = new WordArgument(
            "name", 
            "The name to lookup. Makes a best guess for matching, but for best results use an exact name or intermediate ID (i.e. method_1234 -> 1234).", 
            true) {

        @Override
        public boolean required(Collection<Flag> flags) {
            return !flags.contains(FLAG_FORCE_UPDATE) && !flags.contains(FLAG_DEFAULT_VERSION);
        }
    };
    
    static final Argument<String> ARG_VERSION = new SentenceArgument("version", "The MC version to consider. If not given, will use the default for this guild, or else latest.", false);
    
    private static final Flag FLAG_FORCE_UPDATE = new SimpleFlag('u', "update", "Forces a check for updates before giving results.", false);
    private static final Flag FLAG_DEFAULT_VERSION = new SimpleFlag('v', "version", "Set the default lookup version for this guild. Use \"latest\" to unset. Requires manage server permissions.", true);
    private static final Flag FLAG_CONVERT = new SimpleFlag('c', "convert", "Convert mappings to another set. Valid options: ", true) {
      
        @Override
        public String description() {
            return super.description() + MAPPINGS_MAP.keySet().toString();
        }
    };

    private static final Requirements DEFAULT_VERSION_PERMS = Requirements.builder().with(Permission.MANAGE_GUILD, RequiredType.ALL_OF).build();
    
    private final CommandMappings<M> parent;
    
    protected final MappingType type;
    
    private final String name;
    private final boolean defaultStable;
    private final int color;
    @Getter
    private final MappingDownloader<M, ?> downloader;

    protected CommandMappings(String name, String displayName, boolean defaultStable, int color, MappingDownloader<M, ? extends MappingDatabase<M>> downloader) {
        super(name.toLowerCase(Locale.ROOT), false, () -> "");
        MAPPINGS_MAP.put(name.toLowerCase(Locale.ROOT), this);
        this.parent = null;
        this.type = null;
        this.name = displayName;
        this.defaultStable = defaultStable;
        this.color = color;
        this.downloader = downloader;
    }
    
    protected CommandMappings(String prefix, CommandMappings<M> parent, MappingType type) {
        super(prefix + type.getKey(), false, () -> null);
        this.parent = parent;
        this.type = type;
        this.name = parent.name;
        this.defaultStable = parent.defaultStable;
        this.color = parent.color;
        this.downloader = parent.downloader;
    }

    public static CommandMappings<?> getMappingsCommand(String channel) {
        switch (channel.toLowerCase(Locale.ROOT)) {
            case "stable":
            case "snapshot":
                return MAPPINGS_MAP.get("mcp");
            case "official":
                return MAPPINGS_MAP.get("moj");
            default:
                return null;
        }
    }

    protected abstract CommandMappings<M> createChild(MappingType type);
    
    @Override
    public Iterable<ICommand> getChildren() {
        if (parent == null) {
            return NullHelper.notnullJ(Arrays.stream(MappingType.values()).map(type -> createChild(type)).collect(Collectors.toList()), "Arrays#stream");
        }
        return super.getChildren();
    }
    
    @Override
    public Mono<?> onReady(ReadyContext ctx) {
        if (parent != null || storage == null) {
            return super.onReady(ctx);
        }
        if (parent != null) {
            return parent.onReady(ctx);
        }
        return Mono.empty();
    }
    
    @Override
    public void save(File dataFolder, Gson gson) {
        super.save(dataFolder, gson);
        if (parent != null) {
            parent.save(dataFolder, gson);
        }
    }

    public Mono<String> getMcVersion(CommandContext ctx) {
        return ctx.getArgOrElse(ARG_VERSION, Mono.fromSupplier(() -> storage.get(ctx).orElse(null))
                .filter(s -> !s.isEmpty())
                .switchIfEmpty(downloader.getLatestMinecraftVersion(defaultStable)));
    }

    protected Mono<List<Mapping>> findMappings(CommandContext ctx) {
        final GuildStorage<String> storage = parent == null ? this.storage : parent.storage;
        
        if (ctx.hasFlag(FLAG_DEFAULT_VERSION)) {
            if (!ctx.getGuildId().isPresent()) {
                return ctx.error("Cannot set default version in DMs.").then(Mono.empty());
            }
            if (!DEFAULT_VERSION_PERMS.matches(ctx).block()) {
                return ctx.error("You do not have permission to update the default version!").then(Mono.empty());
            }
            String version = ctx.getFlag(FLAG_DEFAULT_VERSION);
            Mono<String> ret;
            if ("latest".equals(version)) {
                ret = storage.put(ctx, "");
            } else if (downloader.getMinecraftVersions().any(version::equals).block()) {
                ret = storage.put(ctx, version);
            } else {
                return ctx.error("Invalid version.").then(Mono.empty());
            }
            return ret.defaultIfEmpty("latest")
                    .flatMap(prev -> ctx.reply("Changed default version for this guild from " + (prev.isEmpty() ? "latest" : prev) + " to " + version))
                    .then(Mono.empty());
        }

        Mono<String> mcver = getMcVersion(ctx).cache();

        String name = ctx.getArg(ARG_NAME);
        Mono<Void> updateCheck = Mono.empty();
        if (ctx.hasFlag(FLAG_FORCE_UPDATE)) {
            updateCheck = mcver.flatMap(downloader::forceUpdateCheck);
            if (name == null) {
                return updateCheck.then(mcver.flatMap(v -> ctx.reply("Updated mappings for MC " + v))).then(Mono.empty());
            }
        }
        Flux<Mapping> ret = updateCheck.thenMany(mcver.flatMapMany(v -> type == null ? downloader.lookup(name, v) : downloader.lookup(type, name, v)));
        if (ctx.hasFlag(FLAG_CONVERT)) {
            String convertTo = ctx.getFlag(FLAG_CONVERT);
            CommandMappings<?> otherCommand = getOtherCommand(convertTo);
            if (otherCommand == null)
                return ctx.error("Unknown mapping type for conversion: " + convertTo).then(Mono.empty());
            Mono<? extends MappingDatabase<?>> dbCache = mcver.flatMap(otherCommand.downloader::getDatabase).cache();
            ret = ret.flatMap(m -> dbCache.<Mapping>flatMap(db -> Mono.justOrEmpty(m.<Mapping>convert(db))));
        }
        return ret.collectList();
    }

    @Nullable
    private CommandMappings<?> getOtherCommand(String convertTo) {
        return MAPPINGS_MAP.get(convertTo.toLowerCase(Locale.ROOT));
    }

    @Override
    public Mono<?> process(CommandContext ctx) {
        return findMappings(ctx)
                .transform(Monos.flatZipWith(ctx.getChannel(), (mappings, channel) -> {
                    if (!mappings.isEmpty()) {
                        final String title;
                        if (ctx.hasFlag(FLAG_CONVERT)) {
                            CommandMappings<?> otherCommand = getOtherCommand(ctx.getFlag(FLAG_CONVERT));
                            if (otherCommand == null) {
                                title = this.name;
                            } else {
                                title = this.name + " -> " + otherCommand.name;
                            }
                        } else {
                            title = this.name;
                        }
                        return getMcVersion(ctx)
                            .map(v -> new ListMessageBuilder<Mapping>(title + " Mappings")
                                .showIndex(false)
                                .embed(false)
                                .addObjects(mappings)
                                .stringFunc(m -> m.formatMessage(v))
                                .color(color)
                                .build(channel, ctx.getMessage()))
                            .flatMap(PaginatedMessage::send);
                    } else {
                        return ctx.reply("No information found!");
                    }
                }));
    }

    @Override
    public String getDescription(CommandContext ctx) {
        return type == null ? "Looks up " + name + " info." : "Looks up " + name + " info for a given " + type.name().toLowerCase(Locale.US) + ".";
    }

    @Override
    protected TypeToken<String> getDataType() {
        return TypeToken.get(String.class);
    }
}

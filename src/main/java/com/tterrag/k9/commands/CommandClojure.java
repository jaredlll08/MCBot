package com.tterrag.k9.commands;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.security.AccessControlException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.math.NumberUtils;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.tterrag.k9.K9;
import com.tterrag.k9.commands.CommandQuote.Quote;
import com.tterrag.k9.commands.CommandTrick.TrickData;
import com.tterrag.k9.commands.api.Command;
import com.tterrag.k9.commands.api.CommandBase;
import com.tterrag.k9.commands.api.CommandContext;
import com.tterrag.k9.commands.api.Flag;
import com.tterrag.k9.listeners.CommandListener;
import com.tterrag.k9.trick.Trick;
import com.tterrag.k9.util.ActivityUtil;
import com.tterrag.k9.util.BakedMessage;
import com.tterrag.k9.util.EmbedCreator;
import com.tterrag.k9.util.Monos;
import com.tterrag.k9.util.TypeBinding;
import com.tterrag.k9.util.TypeBindingPersistentMap;
import com.tterrag.k9.util.annotation.NonNull;

import clojure.java.api.Clojure;
import clojure.lang.AFn;
import clojure.lang.APersistentMap;
import clojure.lang.ArityException;
import clojure.lang.IFn;
import clojure.lang.LineNumberingPushbackReader;
import clojure.lang.PersistentArrayMap;
import clojure.lang.PersistentVector;
import clojure.lang.Var;
import discord4j.core.DiscordClient;
import discord4j.core.object.Embed;
import discord4j.core.object.Embed.Author;
import discord4j.core.object.Embed.Field;
import discord4j.core.object.Embed.Footer;
import discord4j.core.object.Embed.Image;
import discord4j.core.object.Embed.Provider;
import discord4j.core.object.Embed.Thumbnail;
import discord4j.core.object.Embed.Video;
import discord4j.core.object.entity.Attachment;
import discord4j.core.object.entity.Guild;
import discord4j.core.object.entity.GuildEmoji;
import discord4j.core.object.entity.Member;
import discord4j.core.object.entity.Message;
import discord4j.core.object.entity.Role;
import discord4j.core.object.entity.User;
import discord4j.core.object.entity.channel.GuildChannel;
import discord4j.core.object.entity.channel.MessageChannel;
import discord4j.core.object.entity.channel.TextChannel;
import discord4j.core.object.presence.Activity;
import discord4j.core.object.presence.Presence;
import discord4j.rest.http.client.ClientException;
import discord4j.rest.util.Image.Format;
import discord4j.rest.util.Permission;
import discord4j.common.util.Snowflake;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.Accessors;
import lombok.experimental.Delegate;
import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

@Slf4j
@Command
public class CommandClojure extends CommandBase {
    
    @Value
    private static class ExecutionResult {
        Object result;
        @Accessors(fluent = true)
        boolean deleteSelf;
        
        static ExecutionResult from(APersistentMap map) {
            return new ExecutionResult(map.get(Clojure.read(":res")), (Boolean) map.get(Clojure.read(":delete-self")));
        }
    }

    @Value
    private static class MappingInfo {
        @Delegate
        CommandMappings<?> command;
        String mcVersion;
        boolean stable;
        String channel;
    }
    
    private static final Flag FLAG_NOFN = new SimpleFlag('l', "literal", "Used to force result to be interpreted literally, will not attempt to invoke as a function even if the code returns an IFn", false);
    
    private static final SentenceArgument ARG_EXPR = new SentenceArgument("expression", "The clojure expression to evaluate.", true);
    
    private static final Class<?>[] BLACKLIST_CLASSES = {
            Thread.class
    };
    
    // Blacklist accessing discord functions
    private static final String[] BLACKLIST_PACKAGES = {
            K9.class.getPackage().getName(),
            DiscordClient.class.getPackage().getName(),
    };
    
    private final IFn sandbox;
    
    @SneakyThrows
    public CommandClojure() {
        super("clj", false);

        // Make sure to load in clojail
        Clojure.var("clojure.core", "require").invoke(Clojure.read("[clojail core jvm testers]"));

        // Convenience declarations of used functions
        IFn read_string = Clojure.var("clojure.core", "read-string");
        IFn sandboxfn = Clojure.var("clojail.core", "sandbox");
        Var secure_tester = (Var) Clojure.var("clojail.testers", "secure-tester");
        
        // Load these to add new blacklisted resources
        IFn blacklist_objects = Clojure.var("clojail.testers", "blacklist-objects");
        IFn blacklist_packages = Clojure.var("clojail.testers", "blacklist-packages");

        // Create our tester with custom blacklist
        Object tester = Clojure.var("clojure.core/conj").invoke(secure_tester.getRawRoot(),
                blacklist_objects.invoke(PersistentVector.create((Object[]) BLACKLIST_CLASSES)),
                blacklist_packages.invoke(PersistentVector.create((Object[]) BLACKLIST_PACKAGES)));

        /* == Setting up Context == */

        // Defining all the context vars and the functions to bind them for a given CommandContext

        // A simple function that returns a map representing a user, given an User
        TypeBinding<Member> memberBinding = new TypeBinding<Member>("Member")
                    .bind("id", m -> m.getId().asLong())
                    .bind("name", Member::getUsername)
                    .bind("nick", Member::getDisplayName)
                    .bind("discriminator", Member::getDiscriminator)
                    .bind("bot", Member::isBot)
                    .bind("avatar", Member::getAvatarUrl)
                    .bind("joined", Member::getJoinTime)
                    .bindRecursive("presence", m -> m.getPresence().block(), new TypeBinding<Presence>("Presence")
                        .bind("status", p -> p.getStatus().toString())
                        .bindRecursiveOptional("activity", p -> p.getActivity(), new TypeBinding<Activity>("Activity")
                                .bind("type", a -> a.getType().toString())
                                .bind("name", Activity::getName)
                                .bindOptional("stream_url", Activity::getStreamingUrl, String.class)
                                .bindOptional("start", Activity::getStart, Instant.class)
                                .bindOptional("end", Activity::getEnd, Instant.class)
                                .bindOptional("application_id", a -> a.getApplicationId().map(Snowflake::asLong), Long.class)
                                .bindOptional("details", Activity::getDetails, String.class)
                                .bindOptional("state", Activity::getState, String.class)
                                .bindOptional("party_id", Activity::getPartyId, String.class)
                                .bindOptionalLong("party_size", Activity::getCurrentPartySize)
                                .bindOptionalLong("max_party_size", Activity::getMaxPartySize)
                                .bindOptional("large_image", ActivityUtil::getLargeImageUrl, String.class)
                                .bindOptional("large_text", Activity::getLargeText, String.class)
                                .bindOptional("small_image", ActivityUtil::getSmallImageUrl, String.class)
                                .bindOptional("small_text", Activity::getSmallText, String.class)))
                    .bind("roles", m -> m.getRoles().collectList().map(roles -> roles.stream()
                                .sorted(Comparator.comparing(Role::getRawPosition).reversed())
                                .map(Role::getId)
                                .mapToLong(Snowflake::asLong)
                                .toArray())
                            .block());

        // Set up global context vars

        // Create an easily accessible map for the sending user
        addContextVar("author", ctx -> ctx.getMember().map(m -> TypeBindingPersistentMap.create(memberBinding, m)));

        // Add a lookup function for looking up an arbitrary user in the guild
        addContextVar("users", ctx -> ctx.getGuild()
                .map(guild -> new AFn() {

            @Override
            public Object invoke(Object id) {
                return guild.getClient().getMemberById(guild.getId(), Snowflake.of(((Number)id).longValue()))
                        .map(m -> TypeBindingPersistentMap.create(memberBinding, m))
                        .single()
                        .onErrorMap(NoSuchElementException.class, e -> new IllegalArgumentException("Could not find user for ID"))
                        .block();
            }
            
            @Override
            public String toString() {
                return "Function:\n\tUser ID -> Member\n\nSee Also: `*author*`";
            }
        }));
        
        addContextVar("roles", ctx -> ctx.getGuild().map(guild -> new AFn() {
            
            final TypeBinding<Role> binding = new TypeBinding<Role>("Role")
                    .bind("id", r -> r.getId().asLong())
                    .bind("name", Role::getName)
                    .bind("color", r -> new int[] {r.getColor().getRed(), r.getColor().getGreen(), r.getColor().getBlue()})
                    .bind("hoisted", Role::isHoisted)
                    .bind("mentionable", Role::isMentionable)
                    .bind("everyone", Role::isEveryone);
            
            @Override
            public Object invoke(Object id) {
                Role ret = null;
                if (guild != null) {
                    ret = guild.getRoleById(Snowflake.of(((Number)id).longValue())).block();
                }
                if (ret == null) {
                    throw new IllegalArgumentException("Could not find role for ID");
                }
                return TypeBindingPersistentMap.create(binding, ret);
            }
            
            @Override
            public String toString() {
                return "Function:\n\tRole ID -> Role\n\n" + binding.toString();
            }
        }));
        
        TypeBinding<MessageChannel> channelBinding = new TypeBinding<MessageChannel>("Channel")
              .bind("id", c -> c.getId().asLong())
              .bind("type", c -> c.getType().toString())
              .bindOptional("name", channel -> Optional.of(channel).filter(c -> c instanceof GuildChannel).map(c -> ((GuildChannel) c).getName()), String.class)
              .bindOptional("category", channel -> Optional.of(channel).filter(c -> c instanceof TextChannel).flatMap(c -> ((TextChannel)c).getCategoryId()).map(Snowflake::asLong), long.class)
              .bindOptional("topic", channel -> Optional.of(channel).filter(c -> c instanceof TextChannel).map(c -> ((TextChannel)c).getTopic()), String.class)
              .bindOptional("nsfw", channel -> Optional.of(channel).filter(c -> c instanceof TextChannel).map(c -> ((TextChannel)c).isNsfw()), boolean.class)
              .bindOptional("rate_limit", channel -> Optional.of(channel).filter(c -> c instanceof TextChannel).map(c -> ((TextChannel)c).getRateLimitPerUser()), long.class);

        // Simple data bean representing the current channel
        addContextVar("channel", ctx -> ctx.getChannel().map(c -> TypeBindingPersistentMap.create(channelBinding, c)));
        
        TypeBinding<Guild> guildBinding = new TypeBinding<Guild>("Guild")
                .bind("id", g -> g.getId().asLong())
                .bind("name", Guild::getName)
                .bindOptional("icon", g -> g.getIconUrl(Format.WEB_P), String.class)
                .bindOptional("splash", g -> g.getSplashUrl(Format.WEB_P), String.class)
                .bind("owner", g -> g.getOwnerId().asLong())
                .bind("region", Guild::getRegionId)
                .bind("member_count", Guild::getMemberCount);
        
        // Simple data bean representing the current guild
        addContextVar("guild", ctx -> ctx.getGuild().map(g -> TypeBindingPersistentMap.create(guildBinding, g)));
        
        TypeBinding<Message> messageBinding = new TypeBinding<Message>("Message")
                .bind("id", m -> m.getId().asLong())
                .bind("channel", m -> m.getChannelId().asLong())
                .bindOptional("author", m -> m.getAuthor().map(User::getId).map(Snowflake::asLong), long.class)
                .bind("content", Message::getContent, String.class)
                .bind("timestamp", Message::getTimestamp)
                .bindOptional("edited_timestamp", Message::getEditedTimestamp, Instant.class)
                .bind("tts", Message::isTts)
                .bind("user_mentions", m -> m.getUserMentionIds().stream().mapToLong(Snowflake::asLong).toArray())
                .bind("role_mentions", m -> m.getRoleMentionIds().stream().mapToLong(Snowflake::asLong).toArray())
                .bindRecursiveMany("attachments", Message::getAttachments, new TypeBinding<Attachment>("Attachment")
                        .bind("id", a -> a.getId().asLong())
                        .bind("filename", Attachment::getFilename)
                        .bind("size", Attachment::getSize)
                        .bind("url", Attachment::getUrl)
                        .bind("proxy_url", Attachment::getProxyUrl)
                        .bindOptionalInt("height", Attachment::getHeight)
                        .bindOptionalInt("width", Attachment::getWidth)
                        .bind("spoiler", Attachment::isSpoiler))
                .bindRecursiveMany("embeds", Message::getEmbeds, new TypeBinding<Embed>("Embed")
                        .bindOptional("title", Embed::getTitle, String.class)
                        .bind("type", e -> e.getType().toString())
                        .bindOptional("description", Embed::getDescription, String.class)
                        .bindOptional("url", Embed::getUrl, String.class)
                        .bindOptional("timestamp", Embed::getTimestamp, Instant.class)
                        .bindOptional("color", e -> e.getColor().map(c -> new int[] {c.getRed(), c.getGreen(), c.getBlue()}), int[].class)
                        .bindRecursiveOptional("footer", Embed::getFooter, new TypeBinding<Footer>("Footer")
                                .bind("text", Footer::getText)
                                .bind("icon", Footer::getIconUrl)
                                .bind("icon_proxy_url", Footer::getProxyIconUrl))
                        .bindRecursiveOptional("image", Embed::getImage, new TypeBinding<Image>("Image")
                                .bind("url", Image::getUrl)
                                .bind("proxy_url", Image::getProxyUrl)
                                .bind("height", Image::getHeight)
                                .bind("width", Image::getWidth))
                        .bindRecursiveOptional("thumbnail", Embed::getThumbnail, new TypeBinding<Thumbnail>("Thumbnail")
                                .bind("url", Thumbnail::getUrl)
                                .bind("proxy_url", Thumbnail::getProxyUrl)
                                .bind("height", Thumbnail::getHeight)
                                .bind("width", Thumbnail::getWidth))
                        .bindRecursiveOptional("video", Embed::getVideo, new TypeBinding<Video>("Video")
                                .bind("url", Video::getUrl)
                                .bind("height", Video::getHeight)
                                .bind("width", Video::getWidth))
                        .bindRecursiveOptional("provider", Embed::getProvider, new TypeBinding<Provider>("Provider")
                                .bind("name", Provider::getName)
                                .bindOptional("url", Provider::getUrl, String.class))
                        .bindRecursiveOptional("author", Embed::getAuthor, new TypeBinding<Author>("Author")
                                .bind("name", Author::getName)
                                .bind("url", Author::getUrl)
                                .bind("icon_url", Author::getIconUrl)
                                .bind("icon_proxy_url", Author::getProxyIconUrl))
                        .bindRecursiveMany("fields", Embed::getFields, new TypeBinding<Field>("Field")
                                .bind("name", Field::getName)
                                .bind("value", Field::getValue)
                                .bind("inline", Field::isInline)));

        // Add the current message
        addContextVar("message", ctx -> Mono.just(ctx.getMessage()).map(m -> TypeBindingPersistentMap.create(messageBinding, m)));

        // Provide a lookup function for ID->message
        addContextVar("messages", ctx -> ctx.getGuild()
                .flatMapMany(g -> g.getChannels().ofType(MessageChannel.class))
                .switchIfEmpty(ctx.getChannel())
                .collectList()
                .map(channels -> new AFn() {

            @Override
            public Object invoke(Object arg1) {
                return Flux.fromIterable(channels)
                        .filterWhen(c -> Mono.just(c)
                                .ofType(GuildChannel.class)
                                .flatMap(gc -> gc.getEffectivePermissions(ctx.getAuthorId().get()))
                                .map(p -> p.contains(Permission.VIEW_CHANNEL))
                                .defaultIfEmpty(true))
                        .flatMap(c -> c.getMessageById(Snowflake.of(((Number)arg1).longValue()))
                                .onErrorResume(ClientException.class, $ -> Mono.empty()))
                        .next()
                        .switchIfEmpty(ctx.error(new IllegalArgumentException("No message found")))
                        .map(m -> TypeBindingPersistentMap.create(messageBinding, m))
                        .block();
            }
            
            @Override
            public String toString() {
                return "Function:\n\tMessage ID -> Message\n\nSee Also: `*message*`";
            }
        }));
        
        TypeBinding<GuildEmoji> emoteBinding = new TypeBinding<GuildEmoji>("Emote")
                .bind("id", e -> e.getId().asLong())
                .bind("name", GuildEmoji::getName)
                .bind("animated", GuildEmoji::isAnimated)
                .bind("managed", GuildEmoji::isManaged)
                .bind("requires_colons", GuildEmoji::requiresColons)
                .bind("image", GuildEmoji::getImageUrl)
                .bind("formatted", GuildEmoji::asFormat)
                .bind("roles", e -> e.getRoleIds().stream().mapToLong(Snowflake::asLong).toArray())
                // This is broken in D4J, dummy out for now
                .bindOptional("creator", e -> Optional.empty(), long.class);//e.getUser().onErrorResume($ -> Mono.empty()).blockOptional().map(User::getId), long.class);
        
        addContextVar("emotes", ctx -> ctx.getGuild().map(guild -> new AFn() {
           
            @Override
            public Object invoke() {
                return PersistentVector.create(guild.getEmojiIds().stream().map(Snowflake::asLong).toArray());
            }

            @Override
            public Object invoke(Object arg1) {
                GuildEmoji emote = guild.getGuildEmojiById(Snowflake.of(((Number)arg1).longValue())).block();
                if (emote == null) {
                    throw new IllegalArgumentException("No emote found");
                }
                return TypeBindingPersistentMap.create(emoteBinding, emote);
            }
            
            @Override
            public String toString() {
                return "Function:\n\t() -> list of emote IDs\n\tID -> Emote\n\n" + emoteBinding.toString();
            }
        }));

        // A function for looking up quotes, given an ID, or pass no arguments to return a vector of valid quote IDs
        addContextVar("quotes", ctx -> Mono.justOrEmpty(ctx.getK9().getCommands().findCommand(ctx, "quote"))
                .cast(CommandQuote.class)
                .transform(Monos.mapOptional(cmd -> cmd.getData(ctx)))
                .map(data -> 

            new AFn() {
                
                final TypeBinding<Quote> binding = new TypeBinding<Quote>("Quote")
                        .bind("quote", Quote::getQuote)
                        .bind("quotee", Quote::getQuotee)
                        .bind("owner", Quote::getOwner)
                        .bind("weight", Quote::getWeight)
                        .bind("formatted", q -> q.print(false))
                        .bind("formatted_compact", q -> q.print(true))
                        .bind("id", q -> 0);

                @Override
                public Object invoke() {
                    return PersistentVector.create(data.keySet());
                }
    
                @Override
                public Object invoke(Object arg1) {
                    Quote q = data.get(((Number)arg1).intValue());
                    if (q == null) {
                        throw new IllegalArgumentException("No quote for ID " + arg1);
                    }
                    return TypeBindingPersistentMap.create(binding.bind("id", $ -> arg1), q);
                }

                @Override
                public String toString() {
                    return "Function:\n\t() -> list of quote IDs\n\tID -> Quote\n\n" + binding.toString();
                }
            }
        ));

        // A function for getting mapping data given a mapping channel
        addContextVar("mappings", ctx -> Mono.just(new AFn() {
            final TypeBinding<MappingInfo> mappingBinding = new TypeBinding<MappingInfo>("MappingInfo")
                    .bind("channel", MappingInfo::getChannel)
                    .bind("stable", MappingInfo::isStable)
                    .bind("version", cmd -> cmd.getDownloader().getMappingsVersion(cmd.getMcVersion(), cmd.isStable()).block())
                    .bind("mcversion", MappingInfo::getMcVersion)
                    .bind("mcversions", cmd -> cmd.getDownloader().getMinecraftVersions().collect(Collectors.toSet()).block())
                    .bindRecursive("latest_mcversion", Function.identity(), new TypeBinding<MappingInfo>("LatestMinecraftVersion")
                            .bind("stable", cmd -> cmd.getDownloader().getLatestMinecraftVersion(true).block())
                            .bind("unstable", cmd -> cmd.getDownloader().getLatestMinecraftVersion(false).block()))
                    .bindRecursive("latest_version", Function.identity(), new TypeBinding<MappingInfo>("LatestMappingVersion")
                            .bind("stable", cmd -> cmd.getDownloader().getLatestMappingsVersion(true).block())
                            .bind("unstable", cmd -> cmd.getDownloader().getLatestMappingsVersion(false).block()));

            @Override
            public Object invoke(Object arg1) {
                String channel = (String) arg1;
                CommandMappings<?> command = CommandMappings.getMappingsCommand(channel);
                if (command == null)
                    return null;
                String mcVersion = command.getMcVersion(ctx).block();
                boolean stable = command.getDownloader().getLatestMinecraftVersion(true).block().equals(mcVersion);
                return TypeBindingPersistentMap.create(mappingBinding, new MappingInfo(command, mcVersion, stable, channel));
            }

            @Override
            public String toString() {
                return "Function:\n\tMapping Channel -> MappingInfo\n\n" + mappingBinding;
            }
        }));

        // A function for looking up tricks, given a name. Optionally pass "true" as second param to force global lookup
        addContextVar("tricks", ctx -> Mono.justOrEmpty(ctx.getK9().getCommands().findCommand(ctx, "trick"))
                .cast(CommandTrick.class)
                .transform(Monos.mapOptional(cmd -> cmd.getData(ctx)
                        .map(data -> new AFn() {

            final TypeBinding<TrickData> binding = new TypeBinding<TrickData>("Trick")
                    .bind("type", td -> td.getType().toString())
                    .bind("owner", TrickData::getOwner)
                    .bind("content", TrickData::getInput)
                    .bind("func", td -> null, AFn.class);
            
            @Override
            public Object invoke() {
                return PersistentVector.create(data.keySet());
            }

            @Override
            public Object invoke(Object name) {
                return invoke(name, false);
            }

            @Override
            public Object invoke(Object name, Object global) {
                return TypeBindingPersistentMap.create(binding.bind("func", td -> {
                    Trick t = cmd.getTrick((String) name, ctx.getGuildId().orElse(null), td, (Boolean) global);
                    // Return a function which allows invoking the trick
                    return new AFn() {
    
                        @Override
                        public Object invoke() {
                            return invoke(PersistentVector.create());
                        }
    
                        @Override
                        public Object invoke(Object args) {
                            return t.process(ctx, (Object[]) Clojure.var("clojure.core", "to-array").invoke(args)).block();
                        }
                        
                        @Override
                        public String toString() {
                            return "AFn - Trick Executor";
                        }
                    };
                }, AFn.class), cmd.getTrickData(data, (String) name, (Boolean) global));
            }
            
            @Override
            public String toString() {
                return "Function:\n\t() -> list of trick names\n\tTrick Name -> Trick\n\t(Trick Name, Global) -> Trick\n\n" + binding.toString();
            }
        }))));
        
        // Used only by us, to delete the invoking message after sandbox is finished
        addContextVar("delete-self", ctx -> Mono.just(ThreadLocal.withInitial(() -> false)));
        
        // Create a sandbox, 2000ms timeout, under domain k9.sandbox, and running the sandbox-init.clj script before execution
        this.sandbox = (IFn) sandboxfn.invoke(tester,
                Clojure.read(":timeout"), 2000L,
                Clojure.read(":namespace"), Clojure.read("k9.sandbox"),
                Clojure.read(":refer-clojure"), false,
                Clojure.read(":init"), read_string.invoke(Joiner.on('\n').join(
                        IOUtils.readLines(K9.class.getResourceAsStream("/sandbox-init.clj"), Charsets.UTF_8))));
    }
    
    private final Map<String, Function<@NonNull CommandContext, Mono<?>>> contextVars = new LinkedHashMap<>();
    
    private void addContextVar(String name, Function<@NonNull CommandContext, Mono<?>> factory) {
        String var = "*" + name + "*";
        ((Var) Clojure.var("k9.sandbox", var)).setDynamic().bindRoot(new PersistentArrayMap(new Object[0]));
        contextVars.put(var, factory);
    }
    
    @Override
    public Mono<?> process(CommandContext ctx) {
        return exec(ctx, ctx.getArg(ARG_EXPR))
                .map(msg -> msg.withContent(Strings.nullToEmpty(msg.getContent())))
                .flatMap(ctx::reply);
    }
    
    private static final Pattern SANDBOX_METHOD_NAME = Pattern.compile("sandbox/eval\\d+/fn--\\d+");
        
    public Mono<BakedMessage> exec(CommandContext ctx, String code, Object... args) {
        StringWriter sw = new StringWriter();
        
        final Map<Object, Object> initial = new HashMap<>();
        initial.put(Clojure.var("clojure.core", "*out*"), sw);
        initial.put(Clojure.var("clojure.core", "*in*"), new LineNumberingPushbackReader(new InputStreamReader(System.in)) {
            
            @Override
            public int read() throws IOException {
                throw new UnsupportedOperationException("No standard input available.");
            }
        });

        return Flux.fromIterable(contextVars.entrySet())
            .flatMap(e -> e.getValue().apply(ctx).map(v -> Tuples.of(Clojure.var("k9.sandbox", e.getKey()), v)))
            .collectMap(Tuple2::getT1, Tuple2::getT2, () -> initial)
            .map(bindings -> (APersistentMap) sandbox.invoke(Clojure.read("(exec " + code + " " + ctx.hasFlag(FLAG_NOFN) + " " + parseArgs(args) + ")"), PersistentArrayMap.create(bindings)))
            .onErrorMap(e -> {
                log.error("Clojure error trace: ", e);
                if (e instanceof ExecutionException) {
                    Throwable cause = e.getCause();
                     if (cause != null) {
                         return cause;
                     }
                }
                return e;
            })
            .onErrorResume(TimeoutException.class, $ -> ctx.error("That took too long to execute!"))
            .onErrorResume(e -> e instanceof AccessControlException || e instanceof SecurityException, $ -> ctx.error("Sorry, you're not allowed to do that!"))
            .onErrorResume(e -> e instanceof ArityException && SANDBOX_METHOD_NAME.matcher(((ArityException) e).name).matches(), e -> ctx.error("Incorrect number of arguments (" + ((ArityException) e).actual  + ")"))
            .onErrorResume(e -> e instanceof ArityException && ((ArityException) e).name.equals("sandbox/exec"), e -> ctx.error("Too many closing parentheses."))
            .onErrorResume(ctx::error)
            .map(ExecutionResult::from)
            .flatMap(execResult -> {
                Object res = execResult.getResult();
                BakedMessage msg = new BakedMessage();
                if (res instanceof EmbedCreator.Builder) {
                    msg = msg.withEmbed((EmbedCreator.Builder) res);
                } else if (res instanceof BakedMessage) {
                    msg = (BakedMessage) res;
                } else {
                    if (res == null) {
                        res = sw.getBuffer();
                    }
                    msg = msg.withContent(res.toString());
                }
                if ((msg.getContent() == null || msg.getContent().isEmpty()) && msg.getEmbed() == null) {
                    return ctx.error("Empty result");
                }
                if (execResult.deleteSelf()) {
                    return ctx.getMessage().delete()
                            .thenReturn(msg)
                            .zipWith(ctx.getDisplayName(), (m, name) -> m.withContent((m.getContent() == null ? "\n" : m.getContent() + "\n") + "Sent by: " + name));
                }
                return Mono.just(msg);
            });
    }
    
    private static String parseArgs(Object... args) {
        return Arrays.stream(args)
                .map(CommandClojure::asLiteral)
                .collect(Collectors.joining(" "));
    }
    
    private static String asLiteral(Object arg) {
        if (arg instanceof String) {
            if (NumberUtils.isNumber((String) arg)) {
                // Clojure does not handle java literals perfectly, such as strings like "4D",
                // so we parse and re-encode the number as entirely numeric.
                return NumberUtils.createNumber((String) arg).toString();
            }
            return "\"" + ((String)arg).replace("\"", "\\\"") + "\"";
        }
        if (arg != null) {
            return arg.toString();
        } else {
            return "nil";
        }
    }
    
    @Override
    public String getDescription(CommandContext ctx) {
        return "Evaluate some clojure code in a sandboxed REPL.\n\n"
                + "Available context vars: " + Joiner.on(", ").join(contextVars.keySet().stream().map(s -> "`" + s + "`").iterator()) + "."
                + " Run `" + CommandListener.getPrefix(ctx.getGuildId()) + "clj -l [var]` to preview their contents.";
    }
}

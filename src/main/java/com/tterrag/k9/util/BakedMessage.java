package com.tterrag.k9.util;

import java.io.UnsupportedEncodingException;
import java.util.function.Consumer;

import com.tterrag.k9.commands.api.CommandContext;

import discord4j.core.object.entity.Message;
import discord4j.core.object.entity.MessageChannel;
import discord4j.core.spec.EmbedCreateSpec;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Wither;
import reactor.core.publisher.Mono;

@AllArgsConstructor
@Wither
@DefaultNonNull
@Getter
public class BakedMessage {

    @Nullable
	private final String content;
    @Nullable
	private final EmbedCreator.Builder embed;
	private final boolean tts;

	public BakedMessage() {
		this(null, null, false);
	}

    public Mono<Message> send(MessageChannel channel) {
        return channel.createMessage(m -> {
            if (content != null) {
                try {
                    StringBuilder sb = new StringBuilder();
                    for (byte b : content.getBytes("UTF-32")) {
                        sb.append(String.format("%02x", b));
                    }
                    System.out.println(sb);
                } catch (UnsupportedEncodingException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
                m.setContent(content);
            }
            if (embed != null) {
                m.setEmbed(embed.build());
            }
            m.setTts(tts);
        });
	}
	
	public Mono<Message> update(Message message) {
        return message.edit(m -> {
            if (content != null) {
                m.setContent(content);
            }
            if (embed != null) {
                m.setEmbed(embed.build());
            }
        });
    }
}

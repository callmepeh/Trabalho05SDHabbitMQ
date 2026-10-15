package com.distribuidos;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.util.Random;
import java.util.UUID;

/**
 * Gerador de mensagens.
 *
 * Publica continuamente (>= 5 mensagens por segundo) dois tipos de mensagem
 * no Topic Exchange do RabbitMQ:
 *   - Avaliações de produtos  -> routing key "review.product"
 *   - Solicitações de suporte -> routing key "support.request"
 *
 * A cada 10 segundos também publica uma mensagem MALFORMADA (JSON inválido)
 * em cada routing key, para demonstrar o encaminhamento à DLQ
 * (Dead Letter Queue) pelos consumidores.
 */
public class GeneratorApp {
    private static final String EXCHANGE_NAME = "app.topic.exchange";
    private static final String HOST = System.getenv().getOrDefault("RABBITMQ_HOST", "rabbitmq");

    private static final String[] REVIEWS = {
        "O produto é excelente! Chegou muito rápido e funciona perfeitamente.",
        "Péssima qualidade, quebrou no primeiro dia de uso.",
        "Atende as expectativas, nada de mais nem de menos.",
        "Amei a compra, super recomendo a todos!",
        "Horrível, não comprem, suporte não ajuda em nada."
    };

    private static final String[] SUPPORTS = {
        "Não consigo fazer login na minha conta de acesso.",
        "O sistema está muito lento e demorando para carregar as páginas.",
        "Minha conexão cai toda vez que tento salvar o formulário.",
        "Esqueci minha senha e o e-mail de recuperação não chega.",
        "Erro de timeout de conexão ao conectar com o banco."
    };

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(HOST);

        try (Connection connection = conectarComRetry(factory);
             Channel channel = connection.createChannel()) {

            channel.exchangeDeclare(EXCHANGE_NAME, "topic", true);
            ObjectMapper mapper = new ObjectMapper();
            Random random = new Random();

            System.out.println("Iniciando publicação contínua de mensagens (6 por segundo)...");

            long totalEnviadas = 0;
            long ciclo = 0;

            while (true) {
                for (int i = 0; i < 6; i++) {
                    boolean isReview = random.nextBoolean();
                    String type = isReview ? "review" : "support";
                    String routingKey = isReview ? "review.product" : "support.request";
                    String content = isReview
                        ? REVIEWS[random.nextInt(REVIEWS.length)]
                        : SUPPORTS[random.nextInt(SUPPORTS.length)];

                    Message msg = new Message(type, content);
                    String json = mapper.writeValueAsString(msg);

                    channel.basicPublish(EXCHANGE_NAME, routingKey, null, json.getBytes("UTF-8"));
                    System.out.println(" [x] Enviado [" + routingKey + "] " + msg.getId() + ": " + content);
                    totalEnviadas++;
                }

                // A cada 10s, publica uma mensagem malformada (JSON inválido) em
                // cada routing key: os consumidores não conseguirão processá-la
                // (erro de desserialização) e a encaminharão para a DLQ.
                ciclo++;
                if (ciclo % 10 == 0) {
                    String malformada = "{\"id\": \"" + UUID.randomUUID() + "\", \"content\": JSON-CORROMPIDO";
                    channel.basicPublish(EXCHANGE_NAME, "review.product", null, malformada.getBytes("UTF-8"));
                    channel.basicPublish(EXCHANGE_NAME, "support.request", null, malformada.getBytes("UTF-8"));
                    System.out.println(" [!] Enviada mensagem MALFORMADA (demostração da DLQ) para review.product e support.request");
                }

                if (ciclo % 10 == 0) {
                    System.out.println(" [i] Total de mensagens publicadas até agora: " + totalEnviadas);
                }

                Thread.sleep(1000); // 6 mensagens a cada 1s => 6 msg/s (>= 5 msg/s exigidos)
            }
        }
    }

    private static Connection conectarComRetry(ConnectionFactory factory) throws Exception {
        while (true) {
            try {
                return factory.newConnection();
            } catch (Exception e) {
                System.out.println(" [.] Aguardando o RabbitMQ ficar disponível... (" + e.getMessage() + ")");
                Thread.sleep(2000);
            }
        }
    }
}

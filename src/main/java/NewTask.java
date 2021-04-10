import com.rabbitmq.client.*;

public class NewTask {

    private static final String EXCHANGE_NAME = "direct_logs";

    public static void main(String[] args) throws Exception{
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection conexao = connectionFactory.newConnection();
        Channel canal = conexao.createChannel();

        canal.exchangeDeclare(EXCHANGE_NAME, BuiltinExchangeType.DIRECT);
        String NOME_FILA = canal.queueDeclare().getQueue();
        System.out.println ("[*] Aguardando mensagens. Para sair, pressione CTRL + C");

        if (args.length < 1) {
            System.err.println("Usage: ReceiveLogsDirect [info] [warning] [error]");
            System.exit(1);
        }

        for (String severity : args) {
            canal.queueBind(NOME_FILA, EXCHANGE_NAME, severity);
        }
        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(" [x] Received '" + delivery.getEnvelope().getRoutingKey() + "':'" + message + "'");
        };

        boolean autoAck = true; // ack é feito aqui. Como está autoAck, enviará automaticamente
        canal.basicConsume(NOME_FILA, autoAck, deliverCallback, consumerTag -> {
            System.out.println("Cancelaram a fila: " + NOME_FILA);
        });
    }
}

namespace RabbitMQ.Communication.Contracts
{
    public interface IRabbitMQConfig
    {
        string HostName { get; set; }
        int? Port { get; set; }
        string UserName { get; set; }
        string Password { get; set; }
        string VirtualHost { get; set; }
    }
}

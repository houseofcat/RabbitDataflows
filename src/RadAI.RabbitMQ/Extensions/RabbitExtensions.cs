using RadAI.Utilities;
using RadAI.Utilities.Errors;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using System.Threading.Tasks;

namespace RadAI.RabbitMQ;

public static class RabbitExtensions
{
    public static RabbitOptions GetRabbitOptions(this IConfiguration configuration, string configSectionKey = "RabbitOptions")
    {
        var options = new RabbitOptions();
        configuration.GetSection(configSectionKey).Bind(options);
        return options;
    }

    public static async Task<RabbitOptions> GetRabbitOptionsFromJsonFileAsync(string fileNamePath)
    {
        var rabbitOptions = await JsonFileReader.ReadFileAsync<RabbitOptions>(fileNamePath);
        Guard.AgainstNull(rabbitOptions, nameof(rabbitOptions));

        return rabbitOptions;
    }
}

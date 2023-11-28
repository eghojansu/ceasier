namespace Ceasier.Configuration
{
    public class Project
    {
        public App App { get; set; }

        public Company Company { get; set; }

        public string Title => $"{Company.Short} {App.Name}";

        public string ShortTitle => $"{App.Short}";
    }
}

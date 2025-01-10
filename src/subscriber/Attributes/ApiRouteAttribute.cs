using Microsoft.AspNetCore.Mvc;

namespace subscriber.Attributes;

[AttributeUsage(AttributeTargets.Class)]
public class PublicApiAttribute : RouteAttribute
{
    public PublicApiAttribute() : base("api/[controller]") { }
}

[AttributeUsage(AttributeTargets.Class)]
public class PrivateApiAttribute : RouteAttribute
{
    public PrivateApiAttribute() : base("private/api/[controller]") { }
}
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Xunit;

namespace TestProject1
{
    public class TestContext : DbContext
    {
        private static readonly ILoggerFactory SomeLoggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddDebug();
        });

        public DbSet<Article> Articles { get; set; }
        public DbSet<Tag> Tags { get; set; }
        public DbSet<ArticleTag> ArticleTags { get; set; }

        protected override void OnConfiguring(DbContextOptionsBuilder options)
            => options.UseSqlite("DataSource=:memory:")
                .UseLoggerFactory(SomeLoggerFactory)
                .EnableDetailedErrors()
                .EnableSensitiveDataLogging();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Article>()
                .HasMany(p => p.ArticleTags)
                .WithOne(p => p.Article);

            modelBuilder.Entity<ArticleTag>()
                .HasKey(t => new {t.ArticleId, t.TagId});
        }
    }

    public class Article
    {
        public Guid Id { get; set; }
        public string Label { get; set; }

        public int ReplicationOrder { get; set; }

        public IEnumerable<ArticleTag> ArticleTags { get; set; }
    }

    public class Tag
    {
        public Guid Id { get; set; }
        public string Label { get; set; }

        public int ReplicationOrder { get; set; }
        public IEnumerable<ArticleTag> ArticleTags { get; set; }
    }

    public class ArticleTag
    {
        public int ReplicationOrder { get; set; }

        public Guid ArticleId { get; set; }
        public Article Article { get; set; }

        public Guid TagId { get; set; }
        public Tag Tag { get; set; }
    }

    public class UnitTestClass
    {
        private void PrepareDatabase(TestContext context)
        {
            context.Database.OpenConnection();
            context.Database.EnsureCreated();

            var t1 = new Tag {Id = Guid.NewGuid(), Label = "T1"};
            var t2 = new Tag {Id = Guid.NewGuid(), Label = "T2"};

            var a1 = new Article
            {
                Id = Guid.NewGuid(), Label = "A1", ReplicationOrder = 1,
                ArticleTags = new[] {new ArticleTag {Tag = t1, ReplicationOrder = 1}}
            };
            var a2 = new Article
            {
                Id = Guid.NewGuid(), Label = "A2", ReplicationOrder = 1,
                ArticleTags = new[] {new ArticleTag {Tag = t2, ReplicationOrder = 0}}
            };

            context.Tags.AddRange(new[] {t1, t2});
            context.Articles.AddRange(new[] {a1, a2});

            context.SaveChanges();
        }

        [Fact]
        public void Test1()
        {
            var context = new TestContext();

            PrepareDatabase(context);

            var q1 = context.Articles.Select(a => new {a.Id, a.ReplicationOrder})
                .Union(context.ArticleTags.Select(at => new {Id = at.ArticleId, at.ReplicationOrder}));

            var q2 = q1
                .GroupBy(p => p.Id, p => p.ReplicationOrder, (p, grp) => new {Id = p, ReplicationOrder = grp.Min()});

            var q4 = context.Articles
                .Join(q2, p => p.Id, q => q.Id,
                    (p, s) => new Article
                    {
                        Id = p.Id,
                        Label = p.Label,
                        ArticleTags = p.ArticleTags,
                        ReplicationOrder = s.ReplicationOrder
                    });

            var result = q4.Select(p => new
                {
                    p.Id,
                    p.Label,
                    p.ReplicationOrder,
                    //Tags = string.Join(", ", p.ArticleTags.Select(at => at.Tag.Label))
                })
                .Where(p => p.ReplicationOrder == 0);

            Assert.Single(result.ToList());
        }
        
        [Fact]
        public void Test2()
        {
            var context = new TestContext();

            PrepareDatabase(context);

            var q1 = context.Articles.Select(a => new {a.Id, a.ReplicationOrder})
                .Union(context.ArticleTags.Select(at => new {Id = at.ArticleId, at.ReplicationOrder}));

            var q2 = q1
                .GroupBy(p => p.Id, p => p.ReplicationOrder, (p, grp) => new {Id = p, ReplicationOrder = grp.Min()});

            var q4 = context.Articles
                .Join(q2, p => p.Id, q => q.Id,
                    (p, s) => new Article
                    {
                        Id = p.Id,
                        Label = p.Label,
                        ArticleTags = p.ArticleTags,
                        ReplicationOrder = s.ReplicationOrder
                    });

            var result = q4.Select(p => new
                {
                    p.Id,
                    p.Label,
                    p.ReplicationOrder,
                    Tags = string.Join(", ", p.ArticleTags.Select(at => at.Tag.Label))
                })
                .Where(p => p.ReplicationOrder == 0);

            Assert.Single(result.ToList());
        }
    }
}
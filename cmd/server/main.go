package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"replicated-clob/pkg/api"
	"replicated-clob/pkg/handlers"
	"replicated-clob/pkg/obs"
	"replicated-clob/pkg/replica"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
)

func main() {
	port := flag.Int("port", 0, "port for the HTTP server")
	flag.IntVar(port, "p", 0, "shorthand for --port")
	mode := flag.String("mode", string(replica.NodeRolePrimary), "node mode: primary or secondary")
	flag.StringVar(mode, "m", string(replica.NodeRolePrimary), "shorthand for --mode")
	peers := flag.String("peers", "", "comma-separated peer URLs for primary replication fanout")
	primary := flag.String("primary", "", "primary URL for secondaries")
	flag.Parse()
	if *port == 0 {
		panic("missing required --port (or -p)")
	}

	parsedMode := replica.NodeRole(*mode)
	obs := obs.New()
	ctx, cancel := context.WithCancel(context.Background())

	peerURLs := []string{}
	if trimmedPeers := strings.TrimSpace(*peers); trimmedPeers != "" {
		for _, value := range strings.Split(trimmedPeers, ",") {
			trimmed := strings.TrimSpace(value)
			trimmed = strings.TrimSuffix(trimmed, "/")
			if trimmed != "" {
				peerURLs = append(peerURLs, trimmed)
			}
		}
	}
	replicaCoordinator := replica.NewCoordinator(parsedMode, peerURLs, *primary)
	obs.LogNotice(
		ctx,
		"replica node startup: role=%s peers=%v required_peer_acks=%d can_accept_write=%t primary=%s",
		replicaCoordinator.Role(),
		replicaCoordinator.Peers(),
		replicaCoordinator.RequiredPeerAcks(),
		replicaCoordinator.CanAcceptWrite(),
		replicaCoordinator.Primary(),
	)
	if !replicaCoordinator.CanAcceptWrite() && replicaCoordinator.Primary() == "" {
		obs.LogAlert(ctx, "secondary started without primary; writes will fail until primary is configured")
	}
	if replicaCoordinator.Role() == replica.NodeRolePrimary && len(replicaCoordinator.Peers()) == 0 {
		obs.LogInfo(ctx, "primary started with no peers configured; replication quorums will be single-node mode")
	}

	addr := fmt.Sprintf(":%d", *port)

	app := fiber.New(fiber.Config{
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError

			if strings.Contains(err.Error(), "panic") {
				return c.Status(code).SendString("Internal Server Error")
			}

			var e *fiber.Error
			if errors.As(err, &e) {
				code = e.Code
			}

			c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)

			return c.Status(code).SendString(err.Error())
		},
		EnableTrustedProxyCheck: true,
	})
	app.Use(cors.New())

	handler := handlers.New(obs, replicaCoordinator)

	var router fiber.Router = app

	api.New(router, handler, obs)

	fmt.Printf("Server is live as %s node. Starting to listen.\n", strings.ToUpper(string(parsedMode)))

	sigterm := make(chan os.Signal, 1)
	var wg sync.WaitGroup
	signal.Notify(sigterm, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigterm
		obs.LogNotice(ctx, "Received SIGTERM, shutting down gracefully")
		cancel()

		wg.Add(1)

		time.Sleep(3 * time.Second)
		go func() {
			defer wg.Done()
			if err := app.ShutdownWithTimeout(time.Second * 10); err != nil {
				obs.LogAlert(ctx, "Error shutting down gracefully: %v", err)
			}
		}()
	}()

	go func() {
		if err := app.Listen(addr); err != nil {
			obs.LogAlert(ctx, "Error starting server: %v", err)
		}
	}()

	<-ctx.Done()
	// Wait for the server to shut down cleanly
	wg.Wait()

	obs.LogNotice(ctx, "Server shut down")
}

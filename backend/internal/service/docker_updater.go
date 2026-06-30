package service

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const (
	dockerSocketPath    = "/var/run/docker.sock"
	defaultDockerImage  = "zengguoqin/sub2api:latest"
	tempContainerSuffix = "-next"
)

// DockerUpdater handles Docker-based updates via the Docker socket API.
// It pulls new images and recreates the running container in-place.
// Requires SELF_CONTAINER_NAME env var and /var/run/docker.sock mounted.
type DockerUpdater struct {
	httpClient    *http.Client
	containerName string // SELF_CONTAINER_NAME: name of this running container
	dockerImage   string // DOCKER_IMAGE: base image ref (e.g. ghcr.io/zengguoqin/sub2api:latest)
}

// NewDockerUpdater creates a DockerUpdater from environment variables.
func NewDockerUpdater() *DockerUpdater {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", dockerSocketPath)
		},
	}
	image := os.Getenv("DOCKER_IMAGE")
	if image == "" {
		image = defaultDockerImage
	}
	return &DockerUpdater{
		httpClient:    &http.Client{Transport: transport, Timeout: 10 * time.Minute},
		containerName: os.Getenv("SELF_CONTAINER_NAME"),
		dockerImage:   image,
	}
}

// IsAvailable returns true when the Docker socket is accessible and
// SELF_CONTAINER_NAME is configured.
func (d *DockerUpdater) IsAvailable() bool {
	if d.containerName == "" {
		return false
	}
	_, err := os.Stat(dockerSocketPath)
	return err == nil
}

// GetImage returns the configured base image reference.
func (d *DockerUpdater) GetImage() string {
	return d.dockerImage
}

// GetImageForVersion returns the image ref with the given version tag.
// e.g. "ghcr.io/zengguoqin/sub2api:latest" + "1.2.3" → "ghcr.io/zengguoqin/sub2api:1.2.3"
func (d *DockerUpdater) GetImageForVersion(version string) string {
	base := d.dockerImage
	if idx := strings.LastIndex(base, ":"); idx > 0 && !strings.Contains(base[idx:], "/") {
		base = base[:idx]
	}
	return base + ":" + version
}

// PullImage pulls the specified Docker image via the Docker socket.
// The pull is streamed; any inline error from the daemon is returned.
func (d *DockerUpdater) PullImage(ctx context.Context, image string) error {
	imageName, tag := parseDockerImageRef(image)
	endpoint := fmt.Sprintf("http://localhost/images/create?fromImage=%s&tag=%s",
		url.QueryEscape(imageName), url.QueryEscape(tag))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return fmt.Errorf("build pull request: %w", err)
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("pull %s: %w", image, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("pull failed (HTTP %d): %s", resp.StatusCode, body)
	}

	// Stream pull progress and surface any inline errors from the daemon.
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		var event struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(scanner.Bytes(), &event) == nil && event.Error != "" {
			return fmt.Errorf("pull error: %s", event.Error)
		}
	}
	return scanner.Err()
}

// ScheduleRecreate launches container recreation in a background goroutine.
//
// Recreation flow:
//  1. Inspect current container to copy its HostConfig and networks.
//  2. Create a replacement container (<name>-next) with the new image.
//  3. Start the replacement container.
//  4. Send "stop" to ourselves — our process receives SIGTERM and exits.
//
// The replacement container's entrypoint detects REPLACING_CONTAINER and,
// once healthy, removes the old container record and renames itself to the
// original name so that Caddy/other services find it at the same DNS name.
func (d *DockerUpdater) ScheduleRecreate(image string) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		if err := d.doRecreate(ctx, image); err != nil {
			fmt.Fprintf(os.Stderr, "[docker-update] recreation failed: %v\n", err)
		}
	}()
}

// containerJSON is the subset of Docker's ContainerJSON needed for recreation.
type containerJSON struct {
	Config struct {
		Env    []string          `json:"Env"`
		Labels map[string]string `json:"Labels"`
	} `json:"Config"`
	HostConfig      json.RawMessage `json:"HostConfig"`
	NetworkSettings struct {
		Networks map[string]json.RawMessage `json:"Networks"`
	} `json:"NetworkSettings"`
}

func (d *DockerUpdater) doRecreate(ctx context.Context, newImage string) error {
	// 1. Inspect current container to copy its runtime config.
	current, err := d.inspect(ctx, d.containerName)
	if err != nil {
		return fmt.Errorf("inspect %s: %w", d.containerName, err)
	}

	// 2. Build env for the replacement container.
	//    Strip old self-references and inject fresh ones so the replacement
	//    container's entrypoint knows which container it is replacing.
	newEnv := filterDockerEnv(current.Config.Env, "REPLACING_CONTAINER", "SELF_CONTAINER_NAME")
	newEnv = append(newEnv,
		"REPLACING_CONTAINER="+d.containerName,
		"SELF_CONTAINER_NAME="+d.containerName,
	)

	// 3. Build network endpoints config for the replacement container.
	//    We copy the existing aliases (e.g. the compose service name "sub2api"
	//    which Caddy uses) and ADD the old container name as an extra alias.
	//
	//    Zero-downtime handoff: once the replacement starts, Docker's internal
	//    DNS round-robins between old and new for every alias they share.
	//    Traffic continues to flow during startup. The replacement's entrypoint
	//    stops the old container only AFTER confirming the new one is healthy,
	//    at which point all DNS resolution shifts entirely to the new container.
	endpointsConfig := make(map[string]json.RawMessage, len(current.NetworkSettings.Networks))
	for netName, cfg := range current.NetworkSettings.Networks {
		var ep struct {
			Aliases []string `json:"Aliases"`
		}
		if json.Unmarshal(cfg, &ep) == nil {
			ep.Aliases = appendIfMissing(ep.Aliases, d.containerName)
			if updated, err := json.Marshal(map[string]interface{}{
				"Aliases": ep.Aliases,
			}); err == nil {
				endpointsConfig[netName] = updated
				continue
			}
		}
		endpointsConfig[netName] = cfg
	}

	body, err := json.Marshal(map[string]interface{}{
		"Image":      newImage,
		"Env":        newEnv,
		"HostConfig": current.HostConfig,
		"NetworkingConfig": map[string]interface{}{
			"EndpointsConfig": endpointsConfig,
		},
	})
	if err != nil {
		return fmt.Errorf("marshal container config: %w", err)
	}

	tempName := d.containerName + tempContainerSuffix

	// Clean up any leftover replacement container from a previous failed attempt.
	_ = d.dockerDELETE(context.Background(), "/containers/"+url.PathEscape(tempName)+"?force=true")

	// 4. Create replacement container.
	newID, err := d.createContainer(ctx, tempName, body)
	if err != nil {
		return fmt.Errorf("create replacement container: %w", err)
	}

	// 5. Start replacement container.
	//    As soon as it joins the network with the shared aliases, it begins
	//    receiving traffic alongside the old container (zero-downtime).
	//    The replacement's entrypoint background script waits for its own
	//    health check, then stops+removes the old container and renames itself.
	if err := d.dockerPOST(ctx, "/containers/"+url.PathEscape(newID)+"/start"); err != nil {
		_ = d.dockerDELETE(context.Background(), "/containers/"+url.PathEscape(newID)+"?force=true")
		return fmt.Errorf("start replacement container: %w", err)
	}

	// We do NOT stop the old container here. The replacement's entrypoint
	// handles that after confirming it is healthy, ensuring zero downtime.
	return nil
}

// appendIfMissing appends s to slice only if it is not already present.
func appendIfMissing(slice []string, s string) []string {
	for _, v := range slice {
		if v == s {
			return slice
		}
	}
	return append(slice, s)
}

func (d *DockerUpdater) inspect(ctx context.Context, name string) (*containerJSON, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"http://localhost/containers/"+url.PathEscape(name)+"/json", nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("container %q not found", name)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("inspect returned HTTP %d", resp.StatusCode)
	}
	var info containerJSON
	return &info, json.NewDecoder(resp.Body).Decode(&info)
}

func (d *DockerUpdater) createContainer(ctx context.Context, name string, body []byte) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://localhost/containers/create?name="+url.QueryEscape(name),
		bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := d.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("create returned HTTP %d: %s", resp.StatusCode, msg)
	}
	var result struct {
		ID string `json:"Id"`
	}
	return result.ID, json.NewDecoder(resp.Body).Decode(&result)
}

func (d *DockerUpdater) dockerPOST(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+path, nil)
	if err != nil {
		return err
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return fmt.Errorf("POST %s → HTTP %d: %s", path, resp.StatusCode, msg)
	}
	return nil
}

func (d *DockerUpdater) dockerDELETE(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "http://localhost"+path, nil)
	if err != nil {
		return err
	}
	resp, err := d.httpClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// parseDockerImageRef splits "registry/image:tag" into (image, tag).
// When no tag is present, "latest" is returned.
func parseDockerImageRef(ref string) (image, tag string) {
	if idx := strings.LastIndex(ref, ":"); idx > 0 && !strings.Contains(ref[idx:], "/") {
		return ref[:idx], ref[idx+1:]
	}
	return ref, "latest"
}

// filterDockerEnv returns a copy of envs with keys listed in exclude removed.
func filterDockerEnv(envs []string, exclude ...string) []string {
	out := make([]string, 0, len(envs))
	for _, e := range envs {
		drop := false
		for _, ex := range exclude {
			if strings.HasPrefix(e, ex+"=") {
				drop = true
				break
			}
		}
		if !drop {
			out = append(out, e)
		}
	}
	return out
}

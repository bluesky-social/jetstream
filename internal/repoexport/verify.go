package repoexport

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/bluesky-social/jetstream/segment"
	"github.com/jcalabro/atmos"
	"github.com/jcalabro/atmos/car"
	"github.com/jcalabro/atmos/cbor"
	"github.com/jcalabro/atmos/identity"
	"github.com/jcalabro/atmos/repo"
	atmossync "github.com/jcalabro/atmos/sync"
	"github.com/jcalabro/atmos/xrpc"
	"github.com/jcalabro/gt"
	"github.com/jcalabro/jttp"
)

const (
	rootMismatchMessage          = "local reconstructed MST root does not match authoritative repo root"
	maxAuthoritativeCommitBlocks = 8
)

// VerifyConfig controls authoritative-vs-local repo root verification.
type VerifyConfig struct {
	DataDir string
	DID     string

	// IdentityResolver resolves DID documents so verification can query the
	// account's PDS for PDS-only sync endpoints. Nil uses identity.DefaultResolver.
	IdentityResolver identity.Resolver

	// Selector prunes which segments/blocks reconstruction decodes via the
	// in-memory manifest blooms. Required; see Config.Selector.
	Selector Selector

	// PendingEvents are the live writer's not-yet-flushed events, folded
	// into the local reconstruction so a record created moments ago is
	// reflected before the next compaction flush. See Config.PendingEvents.
	PendingEvents []segment.Event
}

// VerifyReport describes the outcome of comparing a local reconstruction
// against an authoritative repo CAR.
type VerifyReport struct {
	DID               string
	Match             bool
	AuthoritativeRev  string
	AuthoritativeRoot string
	LocalLatestRev    string
	LocalRoot         string
	LocalRecordCount  int
	Message           string
}

// Verify compares cfg.DID's authoritative commit root against a locally
// reconstructed snapshot.
func Verify(ctx context.Context, cfg VerifyConfig) (VerifyReport, error) {
	if cfg.DataDir == "" {
		return VerifyReport{}, errors.New("repoexport: DataDir is required")
	}
	if cfg.DID == "" {
		return VerifyReport{}, errors.New("repoexport: DID is required")
	}
	if err := ctx.Err(); err != nil {
		return VerifyReport{}, err
	}

	authoritativeRev, authoritativeRoot, err := loadAuthoritativeRoot(ctx, cfg.DID, cfg.IdentityResolver)
	if err != nil {
		return VerifyReport{}, err
	}

	report := VerifyReport{
		DID:               cfg.DID,
		AuthoritativeRev:  authoritativeRev,
		AuthoritativeRoot: authoritativeRoot,
	}

	snap, err := Reconstruct(ctx, Config{
		DataDir:       cfg.DataDir,
		DID:           cfg.DID,
		Selector:      cfg.Selector,
		PendingEvents: cfg.PendingEvents,
	})
	if err != nil {
		if errors.Is(err, ErrNoLocalRepo) {
			report.Message = err.Error()
			return report, nil
		}
		return VerifyReport{}, err
	}

	report.LocalLatestRev = snap.LatestRev
	report.LocalRoot = snap.Root.String()
	report.LocalRecordCount = snap.RecordCount
	if authoritativeRoot == report.LocalRoot {
		report.Match = true
		return report, nil
	}

	report.Message = rootMismatchMessage
	return report, nil
}

func loadAuthoritativeRoot(ctx context.Context, did string, resolver identity.Resolver) (string, string, error) {
	pdsURL, err := resolvePDSEndpoint(ctx, did, resolver)
	if err != nil {
		return "", "", err
	}
	xrpcClient := &xrpc.Client{
		Host:       pdsURL,
		HTTPClient: gt.Some(jttp.New(xrpc.BulkDownloadOpts()...)),
	}
	syncClient := atmossync.NewClient(atmossync.Options{Client: xrpcClient})

	rev, commitCID, err := syncClient.GetLatestCommit(ctx, atmos.DID(did))
	if err != nil {
		return "", "", fmt.Errorf("repoexport: get latest authoritative commit for %s: %w", did, err)
	}

	commit, err := loadAuthoritativeCommit(ctx, xrpcClient, did, rev, commitCID)
	if err != nil {
		return "", "", err
	}
	return rev, commit.Data.String(), nil
}

func resolvePDSEndpoint(ctx context.Context, did string, resolver identity.Resolver) (string, error) {
	parsedDID, err := atmos.ParseDID(did)
	if err != nil {
		return "", fmt.Errorf("repoexport: parse DID %q: %w", did, err)
	}
	if resolver == nil {
		resolver = &identity.DefaultResolver{}
	}
	doc, err := resolver.ResolveDID(ctx, parsedDID)
	if err != nil {
		return "", fmt.Errorf("repoexport: resolve authoritative PDS for %s: %w", did, err)
	}
	ident, err := identity.IdentityFromDocument(doc)
	if err != nil {
		return "", fmt.Errorf("repoexport: parse authoritative identity for %s: %w", did, err)
	}
	pdsURL := strings.TrimRight(ident.PDSEndpoint(), "/")
	if pdsURL == "" {
		return "", fmt.Errorf("repoexport: no authoritative PDS endpoint for %s", did)
	}
	return pdsURL, nil
}

func loadAuthoritativeCommit(ctx context.Context, xrpcClient *xrpc.Client, did, rev string, commitCID cbor.CID) (*repo.Commit, error) {
	body, err := xrpcClient.QueryStream(ctx, "com.atproto.sync.getBlocks", map[string]any{
		"did":  did,
		"cids": []string{commitCID.String()},
	})
	if err != nil {
		return nil, fmt.Errorf("repoexport: get authoritative commit block %s for %s: %w", commitCID.String(), did, err)
	}
	defer func() { _ = body.Close() }()

	block, err := readAuthoritativeCommitBlock(body, commitCID)
	if err != nil {
		return nil, err
	}

	var singleCommitCAR bytes.Buffer
	if err := car.WriteAll(&singleCommitCAR, []cbor.CID{commitCID}, []car.Block{block}); err != nil {
		return nil, fmt.Errorf("repoexport: encode authoritative commit block %s: %w", commitCID.String(), err)
	}

	_, commit, err := repo.LoadFromCAR(bytes.NewReader(singleCommitCAR.Bytes()))
	if err != nil {
		return nil, fmt.Errorf("repoexport: decode authoritative commit block %s: %w", commitCID.String(), err)
	}
	if commit.DID != did {
		return nil, fmt.Errorf("repoexport: authoritative commit DID mismatch: latest commit for %s returned commit for %s", did, commit.DID)
	}
	if commit.Rev != rev {
		return nil, fmt.Errorf("repoexport: authoritative commit rev mismatch: getLatestCommit returned %s but commit block contains %s", rev, commit.Rev)
	}
	return commit, nil
}

func readAuthoritativeCommitBlock(r io.Reader, commitCID cbor.CID) (car.Block, error) {
	br := bufio.NewReader(r)

	if err := readCARHeaderAllowEmptyRoots(br); err != nil {
		return car.Block{}, fmt.Errorf("repoexport: read authoritative commit CAR: %w", err)
	}
	for blocksRead := range maxAuthoritativeCommitBlocks {
		block, err := readCARBlock(br)
		if errors.Is(err, io.EOF) {
			return car.Block{}, fmt.Errorf("repoexport: authoritative commit block %s not found in getBlocks response", commitCID.String())
		}
		if err != nil {
			return car.Block{}, fmt.Errorf("repoexport: read authoritative commit block: %w", err)
		}
		if block.CID.Equal(commitCID) {
			return block, nil
		}
		if blocksRead == maxAuthoritativeCommitBlocks-1 {
			break
		}
	}

	return car.Block{}, fmt.Errorf("repoexport: authoritative getBlocks response exceeded %d blocks without commit %s", maxAuthoritativeCommitBlocks, commitCID.String())
}

func readCARHeaderAllowEmptyRoots(br *bufio.Reader) error {
	headerLen, err := binary.ReadUvarint(br)
	if err != nil {
		return fmt.Errorf("reading header length: %w", err)
	}
	if headerLen > car.MaxBlockSize {
		return fmt.Errorf("header length %d exceeds max size", headerLen)
	}

	header := make([]byte, headerLen)
	if _, err := io.ReadFull(br, header); err != nil {
		return fmt.Errorf("reading header: %w", err)
	}

	count, pos, err := cbor.ReadMapHeader(header, 0)
	if err != nil {
		return fmt.Errorf("header: %w", err)
	}

	var hasVersion bool
	for range count {
		key, newPos, err := cbor.ReadText(header, pos)
		if err != nil {
			return fmt.Errorf("header key: %w", err)
		}
		pos = newPos

		switch key {
		case "version":
			hasVersion = true
			ver, newPos, err := cbor.ReadUint(header, pos)
			if err != nil {
				return fmt.Errorf("header version: %w", err)
			}
			if ver != 1 {
				return fmt.Errorf("unsupported version %d, expected 1", ver)
			}
			pos = newPos
		default:
			pos, err = cbor.SkipValue(header, pos)
			if err != nil {
				return fmt.Errorf("skipping header key %q: %w", key, err)
			}
		}
	}
	if !hasVersion {
		return errors.New("header missing 'version'")
	}
	return nil
}

func readCARBlock(br *bufio.Reader) (car.Block, error) {
	blockLen, err := binary.ReadUvarint(br)
	if err != nil {
		return car.Block{}, err
	}
	if blockLen == 0 {
		return car.Block{}, errors.New("zero-length block")
	}
	if blockLen > car.MaxBlockSize {
		return car.Block{}, fmt.Errorf("block length %d exceeds max size", blockLen)
	}

	buf := make([]byte, blockLen)
	if _, err := io.ReadFull(br, buf); err != nil {
		return car.Block{}, fmt.Errorf("reading block: %w", err)
	}
	cid, cidLen, err := cbor.ParseCIDPrefix(buf)
	if err != nil {
		return car.Block{}, fmt.Errorf("parsing block CID: %w", err)
	}
	data := buf[cidLen:]
	expected := cbor.ComputeCID(cid.Codec(), data)
	if !expected.Equal(cid) {
		return car.Block{}, fmt.Errorf("block CID mismatch: claimed %s, computed %s", cid.String(), expected.String())
	}
	return car.Block{CID: cid, Data: data}, nil
}

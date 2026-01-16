<script lang="ts">
  /**
   * Simple line path renderer for card background curves.
   */
  let {
    reals = [],
    used = [],
    width = 300,
    height = 200,
    realsColor = '#ddd',
    usedColor = '#aaa',
    strokeWidth = 1,
    className = ''
  }: {
    reals?: number[];
    used?: number[];
    width?: number;
    height?: number;
    realsColor?: string;
    usedColor?: string;
    strokeWidth?: number;
    className?: string;
  } = $props();

  function normalizeDataWithSharedScale(
    data: number[],
    width: number,
    height: number,
    min: number,
    max: number
  ): { x: number; y: number }[] {
    if (!data || data.length === 0) return [];
    const range = max - min || 1;
    return data.map((value, index) => {
      const x = (index / (data.length - 1)) * width;
      const y = height - ((value - min) / range) * height;
      return { x, y };
    });
  }

  function generatePath(points: { x: number; y: number }[]): string {
    if (points.length === 0) return '';
    return points.map((p, i) => (i === 0 ? `M ${p.x} ${p.y}` : `L ${p.x} ${p.y}`)).join(' ');
  }

  let sharedMin = $derived.by(() => {
    const allValues = [...reals, ...used].filter((v) => v !== undefined && v !== null);
    return allValues.length > 0 ? Math.min(...allValues) : 0;
  });

  let sharedMax = $derived.by(() => {
    const allValues = [...reals, ...used].filter((v) => v !== undefined && v !== null);
    return allValues.length > 0 ? Math.max(...allValues) : 1;
  });

  let realsPoints = $derived(normalizeDataWithSharedScale(reals, width, height, sharedMin, sharedMax));
  let usedPoints = $derived(normalizeDataWithSharedScale(used, width, height, sharedMin, sharedMax));

  let realsPath = $derived(generatePath(realsPoints));
  let usedPath = $derived(generatePath(usedPoints));

  let hasData = $derived(reals.length > 0 || used.length > 0);
</script>

{#if hasData}
  <svg
    xmlns="http://www.w3.org/2000/svg"
    viewBox="0 0 {width} {height}"
    preserveAspectRatio="none"
    class="absolute inset-0 w-full h-full opacity-30 {className}"
    style="z-index: 0;"
  >
    {#if realsPath}
      <path d={realsPath} stroke={realsColor} stroke-width={strokeWidth} fill="none" />
    {/if}
    {#if usedPath}
      <path d={usedPath} stroke={usedColor} stroke-width={strokeWidth} fill="none" />
    {/if}
  </svg>
{/if}

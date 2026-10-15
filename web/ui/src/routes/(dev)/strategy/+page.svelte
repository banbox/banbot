<script lang="ts">
  import { onDestroy, onMount } from 'svelte';
  import { TreeView, type Tree, type Node, buildTree } from '$lib/treeview';
  import Icon from '$lib/Icon.svelte';
  import * as m from '$lib/paraglide/messages.js'
  import { getApi, postApi } from '$lib/netio';
  import { writable } from 'svelte/store';
  import { type Writable } from 'svelte/store';
  import { alerts } from '$lib/stores/alerts';
  import { modals } from '$lib/stores/modals';
  import CodeMirror from '$lib/dev/CodeMirror.svelte';
  import { type Extension } from '@codemirror/state';
  import {oneDark} from '@codemirror/theme-one-dark';
  import { site } from '$lib/stores/site';

  interface FileOp {
    op: string;
    path: string;
    target?: string;
  }

  interface EditTab {
    path: string;
    name: string;
    content: string;
    digest: string;
    saveQueue?: Promise<void>;
  }

  const FILE_OPERATIONS = {
    FOLDER: [
      { key: 'newFile', text: m.new_file(), icon: '(empty)' },
      { key: 'newFolder', text: m.new_folder(), icon: '(empty)' },
      { key: 'rename', text: m.rename(), icon: '(empty)' },
      { key: 'cut', text: m.cut(), icon: 'cut' },
      { key: 'copy', text: m.copy(), icon: 'doc-copy' },
      { key: 'paste', text: m.paste(), icon: 'doc-paste' },
      { key: 'delete', text: m.delete_(), icon: 'trash' }
    ],
    FILE: [
      { key: 'rename', text: m.rename(), icon: '(empty)' },
      { key: 'cut', text: m.cut(), icon: 'cut' },
      { key: 'copy', text: m.copy(), icon: 'doc-copy' },
      { key: 'delete', text: m.delete_(), icon: 'trash' }
    ],
    ROOT: [
      { key: 'newFile', text: m.new_file(), icon: '(empty)' },
      { key: 'newFolder', text: m.new_folder(), icon: '(empty)' },
      { key: 'paste', text: m.paste(), icon: 'doc-paste' }
    ]
  };

  let treeData: Writable<Tree> = writable({
    children: []
  });
  let treeActive = $state('');
  let showCreateInput = $state(false);
  let strategyNameTip = $state('ex: Trend:MaCross');
  let foldName = $state('');
  let strategyNameInput = $state<HTMLInputElement>();
  let clipboardOp: { op: 'cut' | 'copy', sourceId: string } | null = $state(null);
  let tabs: Writable<EditTab[]> = writable([]);
  let activeTab: Writable<number> = writable(0);
  let theme: Extension | null = $state(oneDark);
  let editor: CodeMirror | null = $state(null);
  let showSaveIndicator = $state(false);
  let saveIndicatorTimer: number | null = $state(null);

  // 在 interface EditTab 后添加常量
  const MAX_TABS = 7;

  // 在 script 标签内添加搜索相关的状态
  let searchText = $state('');
  let filteredTreeData: Writable<Tree> = writable({
    children: []
  });

  // 创建一个计算属性来存储过滤后的树
  $effect(() => {
    // dont delete this line
    console.debug($treeData.children.length, searchText)
    setTimeout(() => {
      filteredTreeData.set(filterTree($treeData, searchText));
    }, 0)
  });

  // 添加搜索过滤函数
  function filterTree(tree: Tree, keyword: string): Tree {
    if (!keyword) return tree;
    
    const filtered: Tree = {
      children: []
    };

    // 检查节点是否匹配搜索文本
    function isMatch(node: Node): boolean {
      const fileName = node.name.toLowerCase();
      return fileName.includes(keyword.toLowerCase());
    }

    // 递归检查节点及其子节点
    function processNode(nodeId: string): boolean {
      const node = tree[nodeId];
      if (!node) return false;

      // 复制节点
      filtered[nodeId] = { ...node };
      
      if (node.children) {
        // 处理文件夹
        const matchingChildren = node.children.filter((id: any) => processNode(id));
        if (matchingChildren.length > 0) {
          filtered[nodeId].children = matchingChildren;
          filtered[nodeId].collapsed = false; // 展开包含匹配项的文件夹
          return true;
        }
      }

      // 对于文件，直接检查是否匹配
      const matches = isMatch(node);
      if (!matches) {
        delete filtered[nodeId];
      }
      return matches;
    }

    // 处理根节点的所有子节点
    filtered.children = tree.children.filter(childId => processNode(childId));

    return filtered;
  }


  function handleTreeMenu(node: Node) {
    if (node.id === 'root') {
      return clipboardOp ? FILE_OPERATIONS.ROOT : FILE_OPERATIONS.ROOT.filter(op => op.key !== 'paste');
    }
    const isDir = node.id.endsWith('/');
    return isDir ? FILE_OPERATIONS.FOLDER : FILE_OPERATIONS.FILE;
  }

  async function refreshTree() {
    const result = await getApi('/dev/strat_tree');
    if (result.data) {
      treeData.set(buildTree(result.data));
    }else{
      console.error('refresh tree failed', result);
    }
  }

  async function handleTreeMenuClick(event: { node: Node; key: string }) {
    const { node, key } = event;
    const path = node.id === 'root' ? '' : node.id;

    if (key === 'cut' || key === 'copy') {
      clipboardOp = { op: key, sourceId: path };
      return;
    }

    let data: FileOp = { op: key, path };

    // 处理粘贴操作
    if (key === 'paste') {
      if (!clipboardOp) {
        alerts.error(m.no_paste_src());
        return;
      }
      const operation = clipboardOp.op === 'cut' ? m.cut() : m.copy();
      if (!await modals.confirm(m.confirm_file_target({src: clipboardOp.sourceId, op: operation, target: path}))) {
        return;
      }

      data.target = path;
      data.op = clipboardOp.op;
      data.path = clipboardOp.sourceId;
      clipboardOp = null;
    }

    // 处理需要用户输入的操作
    if (['rename', 'newFile', 'newFolder'].includes(key)) {
      const newName = await prompt(m.input_file_name());
      if (!newName) return;
      data.target = newName;
    }

    if (key === 'delete') {
      if (!await modals.confirm(m.confirm_delete({src: path}))) {
        return;
      }
    }

    let rsp = await postApi('/dev/file_op', data);
    if (rsp.code != 200) {
      alerts.error(rsp.msg || 'operation failed');
      return;
    }else{
      await refreshTree();
    }
  }

  onMount(async () => {
    await refreshTree();
    // 添加 Ctrl+S 保存事件监听
    window.addEventListener('keydown', async (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 's') {
        e.preventDefault();
        alerts.success(m.already_saved());
      }
    });
  });

  onDestroy(() => {
    editor?.flushChanges();
    if ($site.dirtyBin){
      alerts.warning(m.code_modified_need_build(), 2);
      $site.compileNeed = true;
    }
  });

  async function onTreeClick(event: { node: Node; collapsed: boolean }) {
    treeActive = event.node.id;
    if (treeActive.endsWith('/')) {
      foldName = treeActive.split('/').filter(Boolean).pop() || '';
    } else {
      const pathParts = treeActive.split('/');
      pathParts.pop(); // 移除文件名
      foldName = pathParts.pop() || ''; // 获取文件夹名称
    }
    if (foldName) {
      strategyNameTip = `MaCross`;
    } else {
      strategyNameTip = 'ex: Trend:MaCross';
    }

    if (!treeActive.endsWith('/')) {
      await openFile(treeActive);
    }
  }

  async function openFile(path: string) {
    const tabIdx = $tabs.findIndex(tab => tab.path === path);
    const name = path.split('/').pop() || '';
    
    if (tabIdx === -1) {
      // 获取文件内容
      const result = await getApi('/dev/text', { path });
      if (result.code !== 200) {
        alerts.error(result.msg || 'load file failed');
        return;
      }
      const content = result.data || '';
      
      // 检查是否达到最大标签页数量
      if ($tabs.length >= MAX_TABS) {
        editor?.flushChanges();
        // 移除最早的标签页
        tabs.update(tabs => tabs.slice(1));
        if ($activeTab > 0) {
          $activeTab--;
        }
      }
      
      // 添加新标签页
      tabs.update(tabs => [...tabs, { path, name, content, digest: result.digest ?? '' }]);
      $activeTab = $tabs.length - 1;
    } else {
      $activeTab = tabIdx;
    }
    editor?.setValue(path, $tabs[$activeTab].content);
  }

  function clickTab(idx: number) {
    $activeTab = idx;
    openFile($tabs[idx].path);
  }

  function handleCreateClick() {
    showCreateInput = true;
    // 等待DOM更新后聚焦输入框
    setTimeout(() => strategyNameInput?.focus(), 0);
  }

  async function onCreateStrategy() {
    const input = strategyNameInput?.value ?? '';
    const parts = input.split(':');
    
    const namePattern = /^[a-zA-Z][a-zA-Z0-9_]+$/;
    let targetFolder = '';
    let strategyName = '';
    // 检查格式是否为 folder:strategy
    if(!!foldName){
      if(parts.length !== 1) {
        alerts.error(m.only_strat_need());
        return;
      }
      if (treeActive.endsWith('/')) {
        targetFolder = treeActive;
      } else {
        const pathParts = treeActive.split('/');
        pathParts.pop(); // 移除文件名
        targetFolder = pathParts.join('/') + '/'; // 获取文件夹名称
      }
      strategyName = parts[0];
    }else{
      if (parts.length !== 2) {
        alerts.error(m.bad_strat_name());
        return;
      }
      const [folderName, strategy] = parts;
      // 验证文件夹名和策略名
      if (!namePattern.test(folderName)) {
        alerts.error(m.folder() + ': ' + m.bad_strat_ptn());
        return;
      }

      // 在树据中查找目标文件夹
      const target = Object.keys($treeData)
        .find(key => key.endsWith(`${folderName}/`));

      if (!target) {
        alerts.error(m.folder_not_exist({folder: folderName}));
        return;
      }
      targetFolder = target;
      strategyName = strategy;
    }

    if (!namePattern.test(strategyName)) {
      alerts.error(m.strategy() + ': ' + m.bad_strat_ptn());
      return;
    }

    const rsp = await postApi('/dev/new_strat', {folder: targetFolder, name: strategyName});

    if (rsp.code === 200) {
      alerts.success(m.create_strat_ok());
      await refreshTree();
      await openFile(targetFolder+strategyName+".go");
    } else {
      alerts.error(rsp.msg || 'create strategy failed');
    }

    showCreateInput = false;
  }

  function onTextChange(value: string, path?: string) {
    const tab = $tabs.find(tab => tab.path === path);
    if(tab){
      tab.content = value;
      // Serialize this tab's autosaves so each edit uses the last accepted version.
      tab.saveQueue = (tab.saveQueue ?? Promise.resolve()).then(async () => {
        const rsp = await postApi('/dev/save_text', { path: tab.path, content: value, digest: tab.digest });
        if (rsp.code !== 200) {
          console.error('save file failed', rsp);
          alerts.error(rsp.msg || 'save file failed');
        }else{
          tab.digest = rsp.digest;
          if(tab.path.endsWith('.go')){
            $site.dirtyBin = true;
          }
          // 显示保存提示
          showSaveIndicator = true;
          if (saveIndicatorTimer) {
            clearTimeout(saveIndicatorTimer);
          }
          saveIndicatorTimer = setTimeout(() => {
            showSaveIndicator = false;
          }, 2000) as unknown as number;
        }
      });
    }
  }

  function closeTab(idx: number, event: MouseEvent) {
    event.stopPropagation();
    editor?.flushChanges();
    tabs.update(tabs => {
      const newTabs = [...tabs];
      newTabs.splice(idx, 1);
      if ($activeTab === idx) {
        $activeTab = Math.min($activeTab, newTabs.length - 1);
        if (newTabs[$activeTab]) {
          openFile(newTabs[$activeTab].path);
        }
      } else if ($activeTab > idx) {
        $activeTab--;
      }
      return newTabs;
    });
  }

  // 添加关闭所有标签页的函数
  function closeAllTabs() {
    editor?.flushChanges();
    tabs.set([]);
    $activeTab = 0;
  }

</script>

<div class="flex h-svh">
  <!-- 左侧面板 -->
  <div class="w-1/5 border-r border-base-300 flex flex-col p-4 overflow-y-auto">
    <!-- 搜索框 -->
    <fieldset class="fieldset mb-4">
      <div class="input flex items-center gap-2">
        <input 
          type="text" 
          id="search"
          placeholder={m.search()} 
          class="grow bg-transparent border-none outline-none p-0"
          bind:value={searchText}
        />
      </div>
    </fieldset>
    
    <!-- 树形视图 -->
    <div class="flex-1 overflow-y-auto flex flex-col">
      <TreeView 
        tree={$filteredTreeData} 
        active={treeActive} 
        click={onTreeClick} 
        treeClass="gap-1"
        onMenu={handleTreeMenu}
        onMenuClick={handleTreeMenuClick}
      />
    </div>
  </div>

  <!-- 右侧内容 -->
  <div class="w-4/5 flex flex-col overflow-y-auto">
    <!-- 编辑器始终渲染，但通过CSS控制显示/隐藏 -->
    <div class="flex-1 flex flex-col" class:hidden={$tabs.length === 0}>
      <div class="tabs-container border-b border-base-300">
        <div class="tabs flex items-center justify-between">
          <div class="flex items-center gap-[1px]">
            {#each $tabs as tab, i}
              <button 
                class="editor-tab group relative px-4 py-2 border border-base-300 hover:bg-base-200 
                       {i === $activeTab ? 'active bg-base-200 border-t-2 border-t-primary' : 'bg-base-100'}"
                onclick={() => clickTab(i)}
              >
                <span class="text-sm">{tab.name}</span>
                {#if i === $activeTab}
                  <span class="ml-2 text-error" onclick={(e) => closeTab(i, e)}>
                    <Icon name="x" class="w-4 h-4" />
                  </span>
                {/if}
              </button>
            {/each}
          </div>
          <div class="flex items-center gap-2">
            {#if showSaveIndicator}
              <span class="text-primary text-sm">Saved</span>
            {/if}
            {#if $tabs.length > 0}
              <button 
                class="p-2 hover:bg-base-200 rounded-md tooltip tooltip-left" 
                data-tip={m.close_all()}
                onclick={closeAllTabs}
              >
                <Icon name="x" class="w-4 h-4" />
              </button>
            {/if}
          </div>
        </div>
      </div>
      <div class="flex-1 flex flex-col">
        <CodeMirror bind:this={editor} change={onTextChange} {theme} class="flex-1 h-full"/>
      </div>
    </div>

    <!-- 创建策略界面 -->
    {#if $tabs.length === 0}
      <div class="flex-1 flex flex-col items-center justify-center p-8 gap-4">
        <div class="w-[35%] flex flex-col items-center justify-center">
          <h1 class="text-3xl font-bold text-center my-8">{m.strategy()}</h1>
          
          {#if !showCreateInput}
            <button class="btn btn-primary" onclick={handleCreateClick}>
              <Icon name="plus" />
              {m.create_new_strategy()}
            </button>
          {:else}
            <div class="join w-full">
              <label class="input gap-1 flex items-center w-full !outline-none">
                {#if foldName}{foldName}:{/if}
                <input bind:this={strategyNameInput} type="text" placeholder={strategyNameTip} />
              </label>
              <button class="btn btn-primary join-item" onclick={onCreateStrategy}>{m.create()}</button>
            </div>
          {/if}
          
          <div class="divider">{m.or()}</div>
          
          <a href="https://github.com/banbox/banstrats" class="link text-base-content/60 text-center block">
            {m.browse_example_strategy()}
          </a>
        </div>
      </div>
    {/if}
  </div>
</div>

<style>
  :global(main) {
    height: calc(100vh - 4rem);
  }

  .tabs-container {
    background-color: hsl(var(--b1));
  }

  .tabs {
    display: flex;
    gap: 1px;
    padding: 0.5rem 0.5rem 0;
    background-color: hsl(var(--b1));
  }

  .editor-tab {
    display: flex;
    align-items: center;
    max-width: 200px;
    height: 2.25rem;
    transition: all 0.2s;
    position: relative;
    margin-bottom: -1px;
    border-bottom: none;
  }

  .editor-tab.active {
    border-bottom: 1px solid hsl(var(--b2));
  }

  .editor-tab span {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
</style>
